"""Contract migration timing gate (ADR 0232, issue #4610).

A revision whose kind is ``contract`` or ``irreversible`` may not drop or
rename a column or table that the previous released tag's code still names.
During a rolling upgrade the previous release's pods keep running against the
migrated schema, so whatever they name must survive until the release after
the code stops using it.

Every file is read from git objects at refs (the head ref and the tag), never
from the working tree, and no migration module is imported. Only ``upgrade()``
is inspected, together with what it reaches: every module-level function it
names (called or passed as a value, directly or through anything a
module-level statement binds from it, such as an alias), and the nested
functions, lambdas, classes and comprehensions inside that code. Reachability
ignores local shadowing, so a name that could mean a module function reaches
every definition ever bound to it. Downgrade-only code is never visited. A
table or column argument resolves through a module-level string constant only
where no local binding shadows the name (a ``global`` declaration is not a
local binding) and no function rebinds it through ``global``; anything else,
including unpacked ``*``/``**`` arguments, is non-literal.

Exit status: 0 pass, 1 violation (a non-literal dropped name is a violation),
2 usage or git error. There is no allowlist or waiver.
"""

import argparse
import ast
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[1]
KINDS_PATH = "apps/api/src/curie_api/revision_kinds.json"
VERSIONS_DIR = "apps/api/alembic/versions"
CHECKED_KINDS = frozenset({"contract", "irreversible"})
SOURCE_PATHSPECS = (
    ":(glob)apps/*/src/**/*.py",
    ":(glob)packages/*/src/**/*.py",
)
STABLE_TAG = re.compile(r"v(\d+)\.(\d+)\.(\d+)")
PREFIX = "Contract migration timing gate failed:"


class GitError(Exception):
    """A git command failed or its output could not be used."""


class UsageError(Exception):
    """The inputs are unusable (missing ref, file, or revision)."""


@dataclass(frozen=True)
class DroppedObject:
    revision: str
    line: int
    operation: str
    verb: str
    table: str
    column: str | None

    @property
    def searched(self) -> str:
        return self.column if self.column is not None else self.table

    @property
    def rendered(self) -> str:
        return self.table if self.column is None else f"{self.table}.{self.column}"


NOT_LITERAL = "not a string literal or module-level string constant"


@dataclass(frozen=True)
class Nonliteral:
    revision: str
    line: int
    operation: str
    message: str


def _usage(message: str) -> int:
    print(f"Contract migration timing gate error: {message}", file=sys.stderr)
    return 2


class _Git:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root
        self.env = dict(os.environ)
        # A non-git repo root must not resolve an enclosing repository.
        self.env["GIT_CEILING_DIRECTORIES"] = str(repo_root.parent)

    def run(
        self, *args: str, ok_codes: tuple[int, ...] = (0,), stdin: bytes | None = None
    ) -> tuple[int, bytes]:
        try:
            result = subprocess.run(
                ["git", "-C", str(self.repo_root), *args],
                input=stdin,
                capture_output=True,
                env=self.env,
                check=False,
            )
        except OSError as exc:
            raise GitError(f"could not run git: {exc}") from exc
        if result.returncode not in ok_codes:
            detail = result.stderr.decode("utf-8", "replace").strip()
            raise GitError(f"git {' '.join(args)} exited {result.returncode}: {detail}")
        return result.returncode, result.stdout

    def text(self, *args: str) -> str:
        return self.run(*args)[1].decode("utf-8", "replace")

    def commit_of(self, ref: str) -> str:
        code, out = self.run("rev-parse", "-q", "--verify", f"{ref}^{{commit}}", ok_codes=(0, 1))
        if code != 0:
            raise UsageError(f"ref {ref!r} does not name a commit")
        return out.decode().strip()

    def show(self, commit: str, path: str) -> bytes | None:
        code, out = self.run("show", f"{commit}:{path}", ok_codes=(0, 128))
        return out if code == 0 else None


def _previous_stable_tag(git: _Git, head: str) -> str:
    """Newest ``vX.Y.Z`` tag merged into head whose commit is not head itself."""
    listing = git.text(
        "for-each-ref",
        "--merged",
        head,
        "--format=%(refname:short) %(objectname) %(*objectname)",
        "refs/tags",
    )
    best: tuple[tuple[int, int, int], str] | None = None
    for line in listing.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        name = parts[0]
        commit = parts[2] if len(parts) > 2 else parts[1]
        match = STABLE_TAG.fullmatch(name)
        if match is None or commit == head:
            continue
        key = (int(match[1]), int(match[2]), int(match[3]))
        if best is None or key > best[0]:
            best = (key, name)
    if best is None:
        raise UsageError(
            "no stable release tag vX.Y.Z is an ancestor of the head commit; run git fetch --tags"
        )
    return best[1]


def _kinds(git: _Git, commit: str, label: str) -> dict[str, str]:
    raw = git.show(commit, KINDS_PATH)
    if raw is None:
        raise UsageError(f"{KINDS_PATH} is missing at {label}")
    try:
        payload: object = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UsageError(f"{KINDS_PATH} at {label} is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in payload.items()
    ):
        raise UsageError(f"{KINDS_PATH} at {label} is not an object of string kinds")
    return {str(key): str(value) for key, value in payload.items()}


def _version_blobs(git: _Git, commit: str) -> dict[str, bytes]:
    listing = git.run("ls-tree", "-z", commit, "--", f"{VERSIONS_DIR}/")[1]
    blobs: list[tuple[str, str]] = []
    for entry in listing.split(b"\0"):
        if not entry:
            continue
        meta, _, path_bytes = entry.partition(b"\t")
        fields = meta.split()
        path = path_bytes.decode("utf-8", "replace")
        if len(fields) == 3 and fields[1] == b"blob" and path.endswith(".py"):
            blobs.append((path, fields[2].decode()))
    request = "".join(f"{sha}\n" for _, sha in blobs).encode()
    out = git.run("cat-file", "--batch", stdin=request)[1]
    contents: dict[str, bytes] = {}
    offset = 0
    for path, sha in blobs:
        newline = out.index(b"\n", offset)
        header = out[offset:newline].split()
        if len(header) != 3 or header[0].decode() != sha:
            raise GitError(f"unexpected git cat-file output for {path}")
        size = int(header[2])
        start = newline + 1
        contents[path] = out[start : start + size]
        offset = start + size + 1
    return contents


def _assign_targets(node: ast.stmt) -> tuple[list[str], ast.expr | None]:
    if isinstance(node, ast.Assign):
        names = [target.id for target in node.targets if isinstance(target, ast.Name)]
        return names, node.value
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return [node.target.id], node.value
    return [], None


Scope = ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda | ast.ClassDef
Comprehension = ast.ListComp | ast.SetComp | ast.DictComp | ast.GeneratorExp


def _comprehension_parts(node: Comprehension) -> tuple[list[ast.AST], set[str]]:
    """A comprehension's non-target children and the names its targets bind."""
    parts: list[ast.AST] = [node.key, node.value] if isinstance(node, ast.DictComp) else [node.elt]
    targets: set[str] = set()
    for generator in node.generators:
        parts.append(generator.iter)
        parts.extend(generator.ifs)
        targets.update(name.id for name in ast.walk(generator.target) if isinstance(name, ast.Name))
    return parts, targets


def _bound_names(nodes: list[ast.AST]) -> set[str]:
    """Names bound in one scope, without entering nested function or class bodies.

    Comprehension targets bind only inside the comprehension, so they are not
    counted; a walrus inside one still binds the enclosing scope. ``global``
    names are not bindings of the scope.
    """
    names: set[str] = set()
    stack = list(nodes)
    while stack:
        node = stack.pop()
        if isinstance(node, Comprehension):
            stack.extend(_comprehension_parts(node)[0])
            continue
        if isinstance(node, ast.Name) and not isinstance(node.ctx, ast.Load):
            names.add(node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
            continue
        elif isinstance(node, ast.Lambda):
            continue
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.Nonlocal):
            names.update(node.names)
        elif isinstance(node, (ast.ExceptHandler, ast.MatchAs, ast.MatchStar)):
            if node.name is not None:
                names.add(node.name)
        elif isinstance(node, ast.MatchMapping) and node.rest is not None:
            names.add(node.rest)
        stack.extend(ast.iter_child_nodes(node))
    return names


def _scope_body(scope: Scope) -> list[ast.AST]:
    return [scope.body] if isinstance(scope, ast.Lambda) else list(scope.body)


def _scope_parts(scope: Scope) -> tuple[list[ast.AST], list[ast.AST]]:
    """A scope's header children (decorators, defaults, annotations, bases) and its body."""
    body = _scope_body(scope)
    body_ids = {id(node) for node in body}
    header = [child for child in ast.iter_child_nodes(scope) if id(child) not in body_ids]
    return header, body


def _declared_globals(scope: Scope) -> set[str]:
    """Names one scope declares ``global``, without entering nested scopes."""
    names: set[str] = set()
    stack = _scope_body(scope)
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Global):
            names.update(node.names)
        elif not isinstance(node, Scope):
            stack.extend(ast.iter_child_nodes(node))
    return names


def _scope_locals(scope: Scope) -> tuple[set[str], set[str]]:
    """The names a scope binds locally, and the names it declares ``global``."""
    declared = _declared_globals(scope)
    if isinstance(scope, ast.ClassDef):
        return _bound_names(list(scope.body)) - declared, declared
    arguments = scope.args
    params = {arg.arg for arg in (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs)}
    params.update(arg.arg for arg in (arguments.vararg, arguments.kwarg) if arg is not None)
    return (params | _bound_names(_scope_body(scope))) - declared, declared


def _global_rebinds(module: ast.Module) -> set[str]:
    """Module names some nested scope declares ``global`` and assigns."""
    names: set[str] = set()
    for node in ast.walk(module):
        if isinstance(node, Scope):
            names |= _declared_globals(node) & _bound_names(_scope_body(node))
    return names


def _module_revision(module: ast.Module) -> str | None:
    revision: str | None = None
    for node in module.body:
        names, value = _assign_targets(node)
        if "revision" in names and isinstance(value, ast.Constant) and isinstance(value.value, str):
            revision = value.value
    return revision


def _module_bindings(module: ast.Module) -> dict[str, ast.Constant]:
    """Module names whose last binding assigns a constant; others are dropped.

    A bare annotation (``NAME: str``) binds nothing and keeps the earlier value.
    """
    bindings: dict[str, ast.Constant] = {}
    for node in module.body:
        if isinstance(node, ast.AnnAssign) and node.value is None:
            continue
        names, value = _assign_targets(node)
        for name in _bound_names([node]):
            if name in names and isinstance(value, ast.Constant):
                bindings[name] = value
            else:
                bindings.pop(name, None)
    return bindings


def _module_constants(module: ast.Module) -> dict[str, str]:
    rebound = _global_rebinds(module)
    return {
        name: value.value
        for name, value in _module_bindings(module).items()
        if isinstance(value.value, str) and name not in rebound
    }


def _module_functions(
    module: ast.Module,
) -> dict[str, list[ast.FunctionDef | ast.AsyncFunctionDef]]:
    """Every module-level definition of each function name, redefinitions included."""
    functions: dict[str, list[ast.FunctionDef | ast.AsyncFunctionDef]] = {}
    stack: list[ast.AST] = list(reversed(module.body))
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.setdefault(node.name, []).append(node)
        elif isinstance(node, ast.stmt) and not isinstance(node, ast.ClassDef):
            stack.extend(
                reversed([c for c in ast.iter_child_nodes(node) if isinstance(c, ast.stmt)])
            )
    return functions


def _loaded_names(node: ast.AST) -> set[str]:
    """Names a module-level statement reads when it runs (function bodies excluded)."""
    names: set[str] = set()
    stack: list[ast.AST] = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, ast.Name) and isinstance(current.ctx, ast.Load):
            names.add(current.id)
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            stack.extend(_scope_parts(current)[0])
        else:
            stack.extend(ast.iter_child_nodes(current))
    return names


def _module_references(module: ast.Module) -> dict[str, set[str]]:
    """For each module name, every name any module-level statement binding it reads.

    This over-approximates aliasing: ``X = name``, ``X = (name,)[0]`` and a
    decorated ``def X`` all make ``X`` reach what they read.
    """
    references: dict[str, set[str]] = {}
    for statement in module.body:
        loaded = _loaded_names(statement)
        for name in _bound_names([statement]):
            references.setdefault(name, set()).update(loaded)
    return references


def _argument(call: ast.Call, position: int, keyword: str) -> ast.expr | None:
    for kw in call.keywords:
        if kw.arg == keyword:
            return kw.value
    if position < len(call.args) and not any(
        isinstance(arg, ast.Starred) for arg in call.args[: position + 1]
    ):
        return call.args[position]
    return None


class _UpgradeScanner:
    """Collect drops and renames reachable from ``upgrade()``."""

    def __init__(self, revision: str, module: ast.Module) -> None:
        self.revision = revision
        self.constants = _module_constants(module)
        self.functions = _module_functions(module)
        self.references = _module_references(module)
        self.dropped: list[DroppedObject] = []
        self.nonliteral: list[Nonliteral] = []

    def scan(self) -> None:
        if "upgrade" not in self.functions:
            return
        visited: set[str] = set()
        pending = ["upgrade"]
        while pending:
            name = pending.pop(0)
            if name in visited:
                continue
            visited.add(name)
            for function in self.functions[name]:
                callees: list[str] = []
                # Unresolvable batch tables are represented by None.
                self._visit(function, {}, callees, set())
                pending.extend(callee for callee in callees if callee not in visited)

    def _reachable(self, name: str) -> list[str]:
        """Module functions a reference to ``name`` may run, ignoring shadowing."""
        seen = {name}
        pending = [name]
        while pending:
            for target in self.references.get(pending.pop(), set()):
                if target not in seen:
                    seen.add(target)
                    pending.append(target)
        return sorted(target for target in seen if target in self.functions)

    def _resolve(self, node: ast.expr | None, local: set[str]) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name) and node.id not in local:
            return self.constants.get(node.id)
        return None

    def _visit(
        self,
        node: ast.AST,
        batch_tables: dict[str, ast.expr | None],
        callees: list[str],
        local: set[str],
    ) -> None:
        if isinstance(node, Scope):
            # Nested code may run, so scan it; enclosing locals stay shadowed
            # unless the nested scope declares them global. Decorators, defaults,
            # annotations and bases run in the enclosing scope.
            inner_names, declared = _scope_locals(node)
            inner_local = (local - declared) | inner_names
            header, body = _scope_parts(node)
            for child in header:
                self._visit(child, batch_tables, callees, local)
            for child in body:
                self._visit(child, batch_tables, callees, inner_local)
            return
        if isinstance(node, Comprehension):
            # Targets shadow only inside the comprehension. Treating the first
            # iterable as shadowed too only makes more arguments non-literal.
            parts, targets = _comprehension_parts(node)
            for part in parts:
                self._visit(part, batch_tables, callees, local | targets)
            return
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            # Reachability ignores local shadowing on purpose: over-approximate.
            callees.extend(self._reachable(node.id))
            return
        if isinstance(node, (ast.With, ast.AsyncWith)):
            inner = dict(batch_tables)
            for item in node.items:
                self._visit(item.context_expr, batch_tables, callees, local)
                expr = item.context_expr
                if (
                    isinstance(expr, ast.Call)
                    and isinstance(expr.func, ast.Attribute)
                    and isinstance(expr.func.value, ast.Name)
                    and expr.func.value.id == "op"
                    and expr.func.attr == "batch_alter_table"
                    and isinstance(item.optional_vars, ast.Name)
                ):
                    inner[item.optional_vars.id] = _argument(expr, 0, "table_name")
            for statement in node.body:
                self._visit(statement, inner, callees, local)
            return
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                receiver = func.value.id
                if receiver == "op":
                    self._op_call(node, func.attr, local, "op", None)
                elif receiver in batch_tables or receiver == "batch_op":
                    self._op_call(node, func.attr, local, "batch_op", batch_tables.get(receiver))
        for child in ast.iter_child_nodes(node):
            self._visit(child, batch_tables, callees, local)

    def _op_call(
        self,
        call: ast.Call,
        method: str,
        local: set[str],
        receiver: str,
        table_node: ast.expr | None,
    ) -> None:
        batch = receiver == "batch_op"
        operation = f"{receiver}.{method}"
        unpacked = any(isinstance(arg, ast.Starred) for arg in call.args) or any(
            kw.arg is None for kw in call.keywords
        )
        if method == "drop_table" and not batch:
            verb = "drops table"
        elif method == "drop_column":
            verb = "drops column"
        elif method == "alter_column" and (
            unpacked or any(kw.arg == "new_column_name" for kw in call.keywords)
        ):
            verb = "renames column"
        else:
            return
        if unpacked:
            # Unpacking may hide any name, including alter_column's new_column_name.
            message = f"arguments are unpacked, so a name is {NOT_LITERAL}"
            self.nonliteral.append(Nonliteral(self.revision, call.lineno, operation, message))
            return
        if not batch:
            table_node = _argument(call, 0, "table_name")
        table = self._require(call, operation, "table", table_node, local)
        if verb == "drops table":
            if table is not None:
                self.dropped.append(
                    DroppedObject(self.revision, call.lineno, operation, verb, table, None)
                )
            return
        column_node = _argument(call, 0 if batch else 1, "column_name")
        column = self._require(call, operation, "column", column_node, local)
        if table is not None and column is not None:
            self.dropped.append(
                DroppedObject(self.revision, call.lineno, operation, verb, table, column)
            )

    def _require(
        self,
        call: ast.Call,
        operation: str,
        argument: str,
        node: ast.expr | None,
        local: set[str],
    ) -> str | None:
        value = self._resolve(node, local)
        if value is None:
            message = f"{argument} is {NOT_LITERAL}"
            self.nonliteral.append(Nonliteral(self.revision, call.lineno, operation, message))
        return value


def _references(git: _Git, commit: str, name: str) -> list[str]:
    code, out = git.run(
        "grep",
        "-n",
        "-w",
        "-I",
        "-z",
        "--no-color",
        "-F",
        "-e",
        name,
        commit,
        "--",
        *SOURCE_PATHSPECS,
        ok_codes=(0, 1),
    )
    if code == 1:
        return []
    prefix = f"{commit}:".encode()
    hits: list[str] = []
    for record in out.split(b"\n"):
        if not record:
            continue
        if record.startswith(prefix):
            record = record[len(prefix) :]
        fields = record.split(b"\0", 2)
        if len(fields) < 2:
            raise GitError(f"unexpected git grep output: {record[:120]!r}")
        hits.append(f"{fields[0].decode('utf-8', 'replace')}:{fields[1].decode()}")
    return hits


def _new_modules(git: _Git, head: str, new_revisions: list[str]) -> dict[str, ast.Module]:
    modules: dict[str, ast.Module] = {}
    if new_revisions:
        for path, blob in sorted(_version_blobs(git, head).items()):
            try:
                module = ast.parse(blob.decode("utf-8"), filename=path)
            except (SyntaxError, UnicodeDecodeError, ValueError) as exc:
                raise UsageError(f"cannot parse {path} at head: {exc}") from exc
            revision = _module_revision(module)
            if revision in new_revisions:
                modules[revision] = module
    missing = [revision for revision in new_revisions if revision not in modules]
    if missing:
        raise UsageError(f"no file under {VERSIONS_DIR} declares revision " + ", ".join(missing))
    return modules


def _check(git: _Git, head_ref: str | None, previous_tag: str | None) -> tuple[list[str], str, int]:
    """Failure blocks, the compared tag, and how many new contract revisions were checked."""
    head = git.commit_of(head_ref or "HEAD")
    tag_label = previous_tag or _previous_stable_tag(git, head)
    tag = git.commit_of(tag_label)
    head_kinds = _kinds(git, head, head_ref or "HEAD")
    tag_kinds = _kinds(git, tag, tag_label)
    new_revisions = sorted(
        revision
        for revision, kind in head_kinds.items()
        if kind in CHECKED_KINDS and revision not in tag_kinds
    )
    modules = _new_modules(git, head, new_revisions)

    dropped: list[DroppedObject] = []
    nonliteral: list[Nonliteral] = []
    for revision in new_revisions:
        scanner = _UpgradeScanner(revision, modules[revision])
        scanner.scan()
        dropped.extend(scanner.dropped)
        nonliteral.extend(scanner.nonliteral)

    blocks = [
        f"{PREFIX} revision {bad.revision} line {bad.line}: {bad.operation} {bad.message}"
        for bad in sorted(nonliteral, key=lambda n: (n.revision, n.line, n.message))
    ]
    cache: dict[str, list[str]] = {}
    for item in sorted(set(dropped), key=lambda d: (d.revision, d.line, d.rendered, d.operation)):
        if item.searched not in cache:
            cache[item.searched] = _references(git, tag, item.searched)
        hits = cache[item.searched]
        if not hits:
            continue
        lines = [
            f"{PREFIX} revision {item.revision} line {item.line} ({item.operation}) "
            f"{item.verb} {item.rendered}, but code at {tag_label} still names "
            f"{item.searched!r}:"
        ]
        lines.extend(f"  {hit}" for hit in hits)
        blocks.append("\n".join(lines))
    return blocks, tag_label, len(new_revisions)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Fail when a new contract or irreversible Alembic revision drops or "
            "renames a column or table that source at the previous stable "
            "release tag still names (ADR 0232)."
        )
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=DEFAULT_REPO_ROOT,
        help="Repository root (a git checkout with release tags fetched).",
    )
    parser.add_argument(
        "--previous-tag",
        help="Release tag to compare against. Requires --head-ref.",
    )
    parser.add_argument(
        "--head-ref",
        help="Ref holding the candidate revisions. Requires --previous-tag.",
    )
    args = parser.parse_args()
    if (args.previous_tag is None) != (args.head_ref is None):
        parser.print_usage(sys.stderr)
        return _usage("--previous-tag and --head-ref must be given together")
    git = _Git(args.repo_root.resolve())
    try:
        blocks, tag_label, checked = _check(git, args.head_ref, args.previous_tag)
    except (UsageError, GitError) as exc:
        return _usage(str(exc))

    if blocks:
        print("\n".join(blocks), file=sys.stderr)
        print(
            "A contract migration ships one release after the code stops using what "
            "it drops (ADR 0232). Move the drop to the next release.",
            file=sys.stderr,
        )
        return 1
    print(f"contract-migration-timing OK: {checked} contract revision(s) new since {tag_label}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

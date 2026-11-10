#!/usr/bin/env python3
"""Reject private imports and module attributes across API production modules."""

import argparse
import ast
import sys
from dataclasses import dataclass, field
from pathlib import Path

API_PACKAGE = "curie_api"
DEFAULT_ROOT = Path(__file__).resolve().parent.parent / "apps/api/src/curie_api"


def is_private(name: str) -> bool:
    return name.startswith("_") and not (name.startswith("__") and name.endswith("__"))


def is_api_module(name: str) -> bool:
    return name == API_PACKAGE or name.startswith(f"{API_PACKAGE}.")


def module_name(path: Path, root: Path) -> str:
    parts = list(path.relative_to(root).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join((API_PACKAGE, *parts))


@dataclass(frozen=True)
class Problem:
    path: Path
    line: int
    message: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.message}"


@dataclass
class Scope:
    kind: str
    bindings: dict[str, str | None]
    globals: set[str] = field(default_factory=set)
    nonlocals: set[str] = field(default_factory=set)


class BindingCollector(ast.NodeVisitor):
    """Find names local to one scope without collecting nested scope bindings."""

    def __init__(self) -> None:
        self.names: set[str] = set()
        self.globals: set[str] = set()
        self.nonlocals: set[str] = set()
        self.imports: list[ast.Import | ast.ImportFrom] = []

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Store | ast.Del):
            self.names.add(node.id)

    def visit_Import(self, node: ast.Import) -> None:
        self.imports.append(node)
        self.names.update(alias.asname or alias.name.split(".")[0] for alias in node.names)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        self.imports.append(node)
        self.names.update(alias.asname or alias.name for alias in node.names)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.names.add(node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.names.add(node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.names.add(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        pass

    def visit_ListComp(self, node: ast.ListComp) -> None:
        pass

    visit_SetComp = visit_ListComp
    visit_DictComp = visit_ListComp
    visit_GeneratorExp = visit_ListComp

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name:
            self.names.add(node.name)
        self.generic_visit(node)

    def visit_MatchAs(self, node: ast.MatchAs) -> None:
        if node.name:
            self.names.add(node.name)
        self.generic_visit(node)

    def visit_MatchStar(self, node: ast.MatchStar) -> None:
        if node.name:
            self.names.add(node.name)

    def visit_MatchMapping(self, node: ast.MatchMapping) -> None:
        if node.rest:
            self.names.add(node.rest)
        self.generic_visit(node)

    def visit_Global(self, node: ast.Global) -> None:
        self.globals.update(node.names)

    def visit_Nonlocal(self, node: ast.Nonlocal) -> None:
        self.nonlocals.update(node.names)


class ImportChecker(ast.NodeVisitor):
    def __init__(self, path: Path, root: Path, modules: set[str]) -> None:
        self.path = path
        self.module = module_name(path, root)
        self.package = self.module if path.stem == "__init__" else self.module.rpartition(".")[0]
        self.modules = set(modules)
        self.scopes: list[Scope] = []
        self.problems: list[Problem] = []

    def report(self, node: ast.AST, message: str) -> None:
        self.problems.append(Problem(self.path, node.lineno, message))

    def import_source(self, node: ast.ImportFrom) -> str | None:
        if not node.level:
            return node.module or ""
        parts = self.package.split(".")
        if node.level > len(parts):
            return None
        base = parts[: len(parts) - node.level + 1]
        if node.module:
            base.extend(node.module.split("."))
        return ".".join(base)

    def import_bindings(self, node: ast.Import | ast.ImportFrom) -> dict[str, str | None]:
        result: dict[str, str | None] = {}
        if isinstance(node, ast.Import):
            for alias in node.names:
                local = alias.asname or alias.name.split(".")[0]
                result[local] = (
                    alias.name if alias.asname else API_PACKAGE
                ) if is_api_module(alias.name) else None
                if is_api_module(alias.name):
                    parts = alias.name.split(".")
                    self.modules.update(".".join(parts[:i]) for i in range(1, len(parts) + 1))
        else:
            source = self.import_source(node)
            for alias in node.names:
                imported = f"{source}.{alias.name}" if source else ""
                result[alias.asname or alias.name] = (
                    imported if is_api_module(imported) and imported in self.modules else None
                )
        return result

    def new_scope(self, kind: str, body: list[ast.stmt], names: set[str] | None = None) -> Scope:
        collector = BindingCollector()
        for node in body:
            collector.visit(node)
        local = (collector.names | (names or set())) - collector.globals - collector.nonlocals
        scope = Scope(kind, dict.fromkeys(local), collector.globals, collector.nonlocals)
        # Functions may refer to a module import occurring later in their outer scope.
        for node in collector.imports:
            scope.bindings.update(
                (name, module)
                for name, module in self.import_bindings(node).items()
                if name in local
            )
        return scope

    def binding_scope(self, name: str) -> Scope:
        current = self.scopes[-1]
        if name in current.globals:
            return self.scopes[0]
        if name in current.nonlocals:
            for scope in reversed(self.scopes[:-1]):
                if scope.kind == "function" and name in scope.bindings:
                    return scope
        return current

    def resolve_name(self, name: str) -> str | None:
        if name in self.scopes[-1].globals:
            return self.scopes[0].bindings.get(name)
        scopes = self.scopes[:-1] if name in self.scopes[-1].nonlocals else self.scopes
        skip_classes = self.scopes[-1].kind in {"function", "comprehension"}
        for scope in reversed(scopes):
            if scope.kind == "class" and skip_classes:
                continue
            if name in scope.bindings:
                return scope.bindings[name]
            if scope.kind == "function":
                skip_classes = True
        return None

    def resolve_module(self, node: ast.expr) -> str | None:
        if isinstance(node, ast.Name):
            return self.resolve_name(node.id)
        if isinstance(node, ast.Attribute):
            parent = self.resolve_module(node.value)
            qualified = f"{parent}.{node.attr}" if parent else ""
            if qualified in self.modules:
                return qualified
        return None

    def visit_Module(self, node: ast.Module) -> None:
        self.scopes.append(self.new_scope("module", node.body))
        self.generic_visit(node)
        self.scopes.pop()

    def check_module_path(self, node: ast.AST, imported: str) -> None:
        if imported != self.module and is_api_module(imported):
            if any(is_private(part) for part in imported.split(".")[1:]):
                self.report(node, f"private API module import {imported}")

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.check_module_path(node, alias.name)
        for name, module in self.import_bindings(node).items():
            self.binding_scope(name).bindings[name] = module

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        source = self.import_source(node)
        if source is None:
            self.report(node, "relative import escapes the API package")
        elif is_api_module(source):
            for alias in node.names:
                imported = f"{source}.{alias.name}"
                provider = imported if imported in self.modules else source
                if provider != self.module and is_private(alias.name):
                    self.report(node, f"private API import {imported}")
                else:
                    self.check_module_path(node, provider)
        for name, module in self.import_bindings(node).items():
            self.binding_scope(name).bindings[name] = module

    def visit_Attribute(self, node: ast.Attribute) -> None:
        provider = self.resolve_module(node.value)
        if provider and provider != self.module and is_private(node.attr):
            self.report(node, f"private API module attribute {provider}.{node.attr}")
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Store | ast.Del):
            self.binding_scope(node.id).bindings[node.id] = None

    def assign(self, target: ast.expr, provider: str | None) -> None:
        if isinstance(target, ast.Name):
            self.binding_scope(target.id).bindings[target.id] = provider
        else:
            self.visit(target)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        provider = self.resolve_module(node.value)
        for target in node.targets:
            self.assign(target, provider)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self.visit(node.annotation)
        provider = None
        if node.value:
            self.visit(node.value)
            provider = self.resolve_module(node.value)
        self.assign(node.target, provider)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self.visit(node.value)
        self.assign(node.target, self.resolve_module(node.value))

    def visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        for decorator in node.decorator_list:
            self.visit(decorator)
        self.visit(node.args)
        if node.returns:
            self.visit(node.returns)
        for parameter in node.type_params:
            self.visit(parameter)
        self.binding_scope(node.name).bindings[node.name] = None
        arguments = (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
        names = {argument.arg for argument in arguments}
        if node.args.vararg:
            names.add(node.args.vararg.arg)
        if node.args.kwarg:
            names.add(node.args.kwarg.arg)
        self.scopes.append(self.new_scope("function", node.body, names))
        for statement in node.body:
            self.visit(statement)
        self.scopes.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.visit_function(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for expression in (*node.bases, *node.decorator_list, *node.keywords, *node.type_params):
            self.visit(expression)
        self.binding_scope(node.name).bindings[node.name] = None
        self.scopes.append(self.new_scope("class", node.body))
        for statement in node.body:
            self.visit(statement)
        self.scopes.pop()

    def visit_Lambda(self, node: ast.Lambda) -> None:
        self.visit(node.args)
        arguments = (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
        names = {argument.arg for argument in arguments}
        if node.args.vararg:
            names.add(node.args.vararg.arg)
        if node.args.kwarg:
            names.add(node.args.kwarg.arg)
        self.scopes.append(Scope("function", dict.fromkeys(names)))
        self.visit(node.body)
        self.scopes.pop()

    def visit_comprehension_expression(
        self, node: ast.ListComp | ast.SetComp | ast.DictComp | ast.GeneratorExp
    ) -> None:
        self.visit(node.generators[0].iter)
        names = {
            name.id
            for generator in node.generators
            for name in ast.walk(generator.target)
            if isinstance(name, ast.Name)
        }
        self.scopes.append(Scope("comprehension", dict.fromkeys(names)))
        for index, generator in enumerate(node.generators):
            if index:
                self.visit(generator.iter)
            self.visit(generator.target)
            for condition in generator.ifs:
                self.visit(condition)
        if isinstance(node, ast.DictComp):
            self.visit(node.key)
            self.visit(node.value)
        else:
            self.visit(node.elt)
        self.scopes.pop()

    def visit_ListComp(self, node: ast.ListComp) -> None:
        self.visit_comprehension_expression(node)

    def visit_SetComp(self, node: ast.SetComp) -> None:
        self.visit_comprehension_expression(node)

    def visit_DictComp(self, node: ast.DictComp) -> None:
        self.visit_comprehension_expression(node)

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        self.visit_comprehension_expression(node)


def check_sources(sources: dict[Path, str], root: Path) -> list[Problem]:
    modules = {API_PACKAGE}
    for path in sources:
        parts = module_name(path, root).split(".")
        modules.update(".".join(parts[:i]) for i in range(1, len(parts) + 1))
    problems: list[Problem] = []
    for path, source in sorted(sources.items()):
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError as exc:
            problems.append(Problem(path, exc.lineno or 1, f"invalid Python syntax: {exc.msg}"))
            continue
        checker = ImportChecker(path, root, modules)
        checker.visit(tree)
        problems.extend(checker.problems)
    return sorted(problems, key=lambda problem: (problem.path, problem.line, problem.message))


def check_root(root: Path) -> tuple[list[Problem], int]:
    if not root.is_dir():
        return [Problem(root, 1, "API source root is not a directory")], 0
    sources: dict[Path, str] = {}
    problems: list[Problem] = []
    for path in sorted(root.rglob("*.py")):
        try:
            sources[path] = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            problems.append(Problem(path, 1, f"cannot read Python source: {exc}"))
    if not sources and not problems:
        problems.append(Problem(root, 1, "no Python modules found in API source root"))
    problems.extend(check_sources(sources, root))
    return problems, len(sources)


def self_test() -> int:
    fixtures = {
        "__init__.py": "",
        "models.py": "",
        "_internal.py": "",
        "crud/__init__.py": "",
        "crud/agents.py": "",
        "nested/__init__.py": "",
        "nested/helper.py": "",
    }
    cases: list[tuple[str, str, str, list[tuple[int, str]]]] = [
        ("absolute symbol", "consumer.py", "from curie_api.models import _hidden\n",
         [(1, "private API import")]),
        ("relative symbol", "consumer.py", "from .models import _hidden as public\n",
         [(1, "private API import")]),
        ("nested relative symbol", "nested/consumer.py", "from ..models import _hidden\n",
         [(1, "private API import")]),
        ("function local symbol", "consumer.py",
         "def load():\n    from .models import _hidden\n", [(2, "private API import")]),
        ("absolute module alias", "consumer.py",
         "import curie_api.models as m\nm._hidden()\n", [(2, "private API module attribute")]),
        ("double underscore private symbol", "consumer.py",
         "from curie_api.models import __hidden\n", [(1, "private API import")]),
        ("double underscore private attribute", "consumer.py",
         "import curie_api.models as m\nm.__hidden()\n", [(2, "private API module attribute")]),
        ("parent module alias", "consumer.py",
         "from . import models as m\nm._hidden()\n", [(2, "private API module attribute")]),
        ("parent package alias", "consumer.py",
         "from curie_api import crud as c\nc.agents._hidden()\n",
         [(2, "private API module attribute")]),
        ("qualified absolute module", "consumer.py",
         "import curie_api.models\ncurie_api.models._hidden()\n",
         [(2, "private API module attribute")]),
        ("absolute package alias", "consumer.py",
         "import curie_api as api\napi.models._hidden()\n",
         [(2, "private API module attribute")]),
        ("function local module", "consumer.py",
         "def load():\n    from . import models as m\n    return m._hidden()\n",
         [(3, "private API module attribute")]),
        ("assigned module alias", "consumer.py",
         "from . import models\nalias = models\nalias._hidden()\n",
         [(3, "private API module attribute")]),
        ("private absolute module", "consumer.py", "import curie_api._internal as m\n",
         [(1, "private API module import")]),
        ("private module component", "consumer.py",
         "from curie_api._internal import public\n", [(1, "private API module import")]),
        ("private parent module", "consumer.py", "from . import _internal as m\n",
         [(1, "private API import")]),
        ("dotted nested module alias", "consumer.py",
         "import curie_api.crud.agents as a\na._hidden()\n",
         [(2, "private API module attribute")]),
        ("closure module alias", "consumer.py",
         "from . import models as m\ndef outer():\n    def inner():\n        return m._hidden()\n",
         [(4, "private API module attribute")]),
        ("method skips class bindings", "consumer.py",
         "from . import models as m\nclass C:\n    m = object()\n"
         "    def method(self):\n        return m._hidden()\n",
         [(5, "private API module attribute")]),
        ("later global module import", "consumer.py",
         "def load():\n    return m._hidden()\nfrom . import models as m\n",
         [(2, "private API module attribute")]),
        ("public imports", "consumer.py",
         "from .models import public\nfrom . import models as m\nm.public()\n", []),
        ("dunders", "consumer.py",
         "from .models import __all__\nfrom . import models as m\nm.__name__\nm.__version__\n", []),
        ("own module", "consumer.py",
         "from curie_api.consumer import _hidden\n"
         "import curie_api.consumer as own\nown._hidden()\n", []),
        ("own private module from parent", "_internal.py",
         "from curie_api import _internal as own\nown._hidden()\n", []),
        ("ordinary object", "consumer.py",
         "from .models import Thing\nThing._method()\nobject_instance._method()\n", []),
        ("public module object", "consumer.py",
         "from . import models as m\nm.public_object._method()\n", []),
        ("external internals", "consumer.py",
         "from outside._internal import _hidden\nimport outside as m\nm._hidden()\n", []),
        ("argument shadows module", "consumer.py",
         "from . import models as m\ndef load(m):\n    return m._method()\n", []),
        ("local object shadows module", "consumer.py",
         "from . import models as m\ndef load():\n    m = object()\n    return m._method()\n", []),
        ("comprehension object", "consumer.py",
         "from . import models as m\n[m._method() for m in objects]\n", []),
        ("lambda argument", "consumer.py",
         "from . import models as m\ncallback = lambda m: m._method()\n", []),
        ("local import does not escape", "consumer.py",
         "def load():\n    from . import models as m\n    return m.public\nm._method()\n", []),
        ("malformed syntax", "consumer.py", "def broken(:\n", [(1, "invalid Python syntax")]),
    ]
    root = Path(API_PACKAGE)
    for label, relative_path, source, expected in cases:
        sources = {root / path: text for path, text in fixtures.items()}
        sources[root / relative_path] = source
        problems = check_sources(sources, root)
        matches = len(problems) == len(expected) and all(
            problem.line == line and message in problem.message
            for problem, (line, message) in zip(problems, expected, strict=False)
        )
        if not matches:
            print(f"Private import self test failed: {label}", file=sys.stderr)
            for problem in problems:
                print(problem, file=sys.stderr)
            print(f"Expected: {expected}", file=sys.stderr)
            return 1
    print(f"Private import self test passed: {len(cases)} cases")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    problems, checked = check_root(args.root)
    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1
    print(f"Private API imports: zero violations across {checked} modules")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

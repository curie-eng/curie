"""Discover worker SQL without importing it, then plan it against the real schema."""

from __future__ import annotations

import ast
import itertools
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection

type _Values = tuple[str, ...]
type _Environment = dict[str, _Values]


def _path(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _path(node.value)
        if parent is not None:
            return f"{parent}.{node.attr}"
    return None


def _unique(values: list[str]) -> _Values:
    return tuple(dict.fromkeys(values))


def _merge(environments: list[_Environment]) -> _Environment:
    """Retain every alternative known in every possible control flow arm."""

    if not environments:
        return {}
    known = set(environments[0]).intersection(*(set(item) for item in environments[1:]))
    return {
        key: _unique([value for environment in environments for value in environment[key]])
        for key in known
    }


def _strings(node: ast.expr, environment: _Environment) -> _Values:
    """Resolve only strings and their explicitly supported static constructions."""

    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return (node.value,)
    name = _path(node)
    if name is not None and name in environment:
        return environment[name]
    if isinstance(node, ast.IfExp):
        return _unique([*_strings(node.body, environment), *_strings(node.orelse, environment)])
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _unique(
            [
                left + right
                for left, right in itertools.product(
                    _strings(node.left, environment), _strings(node.right, environment)
                )
            ]
        )
    if isinstance(node, ast.JoinedStr):
        parts: list[_Values] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append((value.value,))
            elif isinstance(value, ast.FormattedValue):
                rendered = _strings(value.value, environment)
                if value.conversion == ord("r"):
                    rendered = tuple(repr(item) for item in rendered)
                elif value.conversion == ord("a"):
                    rendered = tuple(ascii(item) for item in rendered)
                elif value.conversion not in {-1, ord("s")}:
                    raise ValueError("unsupported fstring conversion")
                if value.format_spec is not None:
                    specifications = _strings(value.format_spec, environment)
                    rendered = tuple(
                        format(item, specification)
                        for item, specification in itertools.product(rendered, specifications)
                    )
                parts.append(rendered)
            else:
                raise ValueError("unsupported fstring component")
        return _unique(["".join(values) for values in itertools.product(*parts)])
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "format"
    ):
        templates = _strings(node.func.value, environment)
        arguments = [_strings(argument, environment) for argument in node.args]
        keywords: dict[str, _Values] = {}
        for keyword in node.keywords:
            if keyword.arg is None:
                raise ValueError("unresolved expanded format arguments")
            keywords[keyword.arg] = _strings(keyword.value, environment)
        names = list(keywords)
        values: list[str] = []
        for template in templates:
            for arguments_variant in itertools.product(*arguments):
                for keywords_variant in itertools.product(*(keywords[name] for name in names)):
                    values.append(
                        template.format(
                            *arguments_variant, **dict(zip(names, keywords_variant, strict=True))
                        )
                    )
        return _unique(values)
    raise ValueError(f"unresolved SQL expression {ast.unparse(node)}")


def _assign(target: ast.expr, value: ast.expr, environment: _Environment) -> None:
    name = _path(target)
    if name is None:
        return
    # A symbolic config object is tracked by its known attributes. Copying the
    # object preserves those attributes without instantiating application code.
    source = _path(value)
    aliases = {
        f"{name}{key[len(source) :]}": values
        for key, values in environment.items()
        if source is not None and key.startswith(f"{source}.")
    }
    try:
        resolved = _strings(value, environment)
    except (ValueError, KeyError, IndexError):
        resolved = None
    for key in list(environment):
        if key == name or key.startswith(f"{name}."):
            del environment[key]
    if resolved is not None:
        environment[name] = resolved
    environment.update(aliases)


def _seed(function: ast.FunctionDef | ast.AsyncFunctionDef, schema: str) -> _Environment:
    environment: _Environment = {}
    arguments = [*function.args.posonlyargs, *function.args.args, *function.args.kwonlyargs]
    for argument in arguments:
        if argument.arg in {"schema", "db_schema"}:
            environment[argument.arg] = (schema,)
        elif argument.arg == "config":
            environment["config.db_schema"] = (schema,)
    return environment


def _constructors(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module in {"sqlalchemy", "sqlalchemy.sql", "sqlalchemy.sql.expression"}:
                names.update(
                    alias.asname or alias.name for alias in node.names if alias.name == "text"
                )
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "sqlalchemy" or alias.name.startswith("sqlalchemy.sql"):
                    prefix = alias.asname or alias.name
                    names.add(f"{prefix}.text")
                    if alias.name == "sqlalchemy" and alias.asname is None:
                        names.add("sqlalchemy.sql.text")
    return names


class _SQLVisitor(ast.NodeVisitor):
    def __init__(self, relative: str, schema: str, constructors: set[str]) -> None:
        self.relative = relative
        self.schema = schema
        self.constructors = constructors
        self.environment: _Environment = {}
        self.class_environment: _Environment = {}
        self.scopes: list[str] = []
        self.statements: list[tuple[str, str]] = []

    def visit_Module(self, node: ast.Module) -> None:
        # Module constants can be declared below a function that uses them.
        for statement in node.body:
            if isinstance(statement, ast.Assign):
                for target in statement.targets:
                    _assign(target, statement.value, self.environment)
            elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
                _assign(statement.target, statement.value, self.environment)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        previous_class = self.class_environment
        attributes: _Environment = {}
        initializer = next(
            (
                method
                for method in node.body
                if isinstance(method, ast.FunctionDef) and method.name == "__init__"
            ),
            None,
        )
        if initializer is not None:
            attributes = {**self.environment, **_seed(initializer, self.schema)}
            for statement in initializer.body:
                if isinstance(statement, ast.Assign):
                    for target in statement.targets:
                        _assign(target, statement.value, attributes)
                elif isinstance(statement, ast.AnnAssign) and statement.value is not None:
                    _assign(statement.target, statement.value, attributes)
            attributes = {
                key: value for key, value in attributes.items() if key.startswith("self.")
            }
        self.class_environment = attributes
        self.scopes.append(node.name)
        for statement in node.body:
            self.visit(statement)
        self.scopes.pop()
        self.class_environment = previous_class

    def _function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        previous = self.environment
        self.environment = {**previous, **self.class_environment}
        for argument in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]:
            self.environment.pop(argument.arg, None)
        self.environment.update(_seed(node, self.schema))
        self.scopes.append(node.name)
        for statement in node.body:
            self.visit(statement)
        self.scopes.pop()
        self.environment = previous

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._function(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        for target in node.targets:
            _assign(target, node.value, self.environment)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self.visit(node.value)
            _assign(node.target, node.value, self.environment)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self.visit(node.value)
        value = ast.BinOp(left=node.target, op=node.op, right=node.value)
        _assign(node.target, value, self.environment)

    def _forget(self, target: ast.expr) -> None:
        for child in ast.walk(target):
            if isinstance(child, ast.Name):
                for key in list(self.environment):
                    if key == child.id or key.startswith(f"{child.id}."):
                        del self.environment[key]

    def visit_For(self, node: ast.For) -> None:
        self.visit(node.iter)
        before = self.environment.copy()
        self._forget(node.target)
        self._loop(node.body, node.orelse, before)

    def _loop(self, body: list[ast.stmt], otherwise: list[ast.stmt], before: _Environment) -> None:
        for statement in body:
            self.visit(statement)
        self.environment = _merge([before, self.environment])
        for statement in otherwise:
            self.visit(statement)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
        self.visit(node.iter)
        before = self.environment.copy()
        self._forget(node.target)
        self._loop(node.body, node.orelse, before)

    def visit_While(self, node: ast.While) -> None:
        self.visit(node.test)
        self._loop(node.body, node.orelse, self.environment.copy())

    def visit_With(self, node: ast.With) -> None:
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self._forget(item.optional_vars)
        for statement in node.body:
            self.visit(statement)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self._forget(item.optional_vars)
        for statement in node.body:
            self.visit(statement)

    def visit_If(self, node: ast.If) -> None:
        self.visit(node.test)
        before = self.environment.copy()
        for statement in node.body:
            self.visit(statement)
        body = self.environment.copy()
        self.environment = before.copy()
        for statement in node.orelse:
            self.visit(statement)
        otherwise = self.environment
        self.environment = _merge([body, otherwise])

    def _try(self, node: ast.Try | ast.TryStar) -> None:
        before = self.environment.copy()
        prefixes = [before]
        for statement in node.body:
            self.visit(statement)
            prefixes.append(self.environment.copy())
        for statement in node.orelse:
            self.visit(statement)
        outcomes = [self.environment.copy()]
        # An exception can occur before any later assignment in the try body.
        # Unknown values on one such path must not be replaced by another arm.
        handler_input = _merge(prefixes)
        for handler in node.handlers:
            self.environment = handler_input.copy()
            if handler.type is not None:
                self.visit(handler.type)
            if handler.name is not None:
                self._forget(ast.Name(id=handler.name, ctx=ast.Store()))
            for statement in handler.body:
                self.visit(statement)
            outcomes.append(self.environment.copy())
        self.environment = _merge(outcomes)
        for statement in node.finalbody:
            self.visit(statement)

    def visit_Try(self, node: ast.Try) -> None:
        self._try(node)

    def visit_TryStar(self, node: ast.TryStar) -> None:
        self._try(node)

    def visit_Match(self, node: ast.Match) -> None:
        self.visit(node.subject)
        before = self.environment.copy()
        outcomes: list[_Environment] = []
        exhaustive = False
        for case in node.cases:
            self.environment = before.copy()
            for pattern in ast.walk(case.pattern):
                if isinstance(pattern, ast.MatchAs | ast.MatchStar) and pattern.name is not None:
                    self._forget(ast.Name(id=pattern.name, ctx=ast.Store()))
                elif isinstance(pattern, ast.MatchMapping) and pattern.rest is not None:
                    self._forget(ast.Name(id=pattern.rest, ctx=ast.Store()))
            if case.guard is not None:
                self.visit(case.guard)
            for statement in case.body:
                self.visit(statement)
            outcomes.append(self.environment.copy())
            exhaustive |= (
                case.guard is None
                and isinstance(case.pattern, ast.MatchAs)
                and case.pattern.pattern is None
            )
        if not exhaustive:
            outcomes.append(before)
        self.environment = _merge(outcomes)

    def visit_Call(self, node: ast.Call) -> None:
        if _path(node.func) in self.constructors:
            site = f"{self.relative}:{node.lineno}"
            scope = ".".join(self.scopes) or "module"
            if len(node.args) != 1 or node.keywords:
                raise ValueError(f"{site} ({scope}): unsupported SQL text arguments")
            try:
                variants = _strings(node.args[0], self.environment)
            except (ValueError, KeyError, IndexError) as error:
                raise ValueError(f"{site} ({scope}): {error}") from error
            if not variants:
                raise ValueError(f"{site} ({scope}): SQL expression has no variants")
            for index, sql in enumerate(variants, start=1):
                identity = site if len(variants) == 1 else f"{site}:{index}"
                self.statements.append((identity, sql))
        self.generic_visit(node)


def discover_statements(root: Path, schema: str) -> list[tuple[str, str]]:
    """Discover every worker SQLAlchemy text call and every static string variant."""

    statements: list[tuple[str, str]] = []
    for path in sorted((root / "apps/worker/src/curie_worker").rglob("*.py")):
        relative = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=relative)
        visitor = _SQLVisitor(relative, schema, _constructors(tree))
        visitor.visit(tree)
        statements.extend(visitor.statements)
    return statements


async def explain_statements(
    connection: AsyncConnection, statements: list[tuple[str, str]]
) -> None:
    """Plan each statement without executing it or mutating the migrated database."""

    for site, sql in statements:
        # SQL NULL values avoid indeterminate parameter types for clauses such as
        # :requested_id IS NULL. The dialect compiler handles casts and quoted
        # strings, so no textual replacement can alter a literal or SQL comment.
        compiled = text(sql).compile(
            dialect=connection.dialect, compile_kwargs={"literal_binds": True}
        )
        try:
            await connection.exec_driver_sql(f"EXPLAIN {compiled}")
        except DBAPIError as error:
            error.add_note(f"worker SQL source: {site}")
            raise

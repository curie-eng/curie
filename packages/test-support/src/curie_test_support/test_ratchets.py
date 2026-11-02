"""Generated, shrinking baselines for test-time costs."""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

BASELINE = "tests/baselines/test-time.json"
BUDGET_SECONDS = 20
Function = ast.FunctionDef | ast.AsyncFunctionDef


def canonical_nodeid(nodeid: str) -> str:
    # xdist loadgroup appends a scheduling identity after the real node id.
    return nodeid.rsplit("@", 1)[0] if nodeid.rfind("@") > nodeid.rfind("]") else nodeid


def read_baseline(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict) or set(value) != {"sleeps", "fixtures", "durations"}:
        raise ValueError("baseline must contain sleeps, fixtures and durations")
    if not isinstance(value["sleeps"], dict) or any(
        not isinstance(key, str) or type(count) is not int or count < 1
        for key, count in value["sleeps"].items()
    ):
        raise ValueError("sleeps must map files to positive counts")
    for name in ("fixtures", "durations"):
        entries = value[name]
        if not isinstance(entries, list) or any(not isinstance(item, str) for item in entries):
            raise ValueError(f"{name} must be a list of identities")
        if entries != sorted(set(entries)):
            raise ValueError(f"{name} must be sorted and unique")
    return value


def python_files(root: Path) -> list[Path]:
    tracked = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "*.py"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if tracked.returncode == 0:
        return [
            root / name
            for name in sorted(set(tracked.stdout.splitlines()))
            if (root / name).is_file()
        ]
    return sorted(root.rglob("*.py"))


def is_test(path: Path) -> bool:
    return (
        path.name == "conftest.py"
        or path.name.startswith("test_")
        or path.name.endswith("_test.py")
    )


def dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = dotted(node.value)
        return f"{prefix}.{node.attr}" if prefix else ""
    return ""


def imports(tree: ast.AST) -> dict[str, str]:
    result: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                result[alias.asname or alias.name.split(".")[0]] = alias.name
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                result[alias.asname or alias.name] = (
                    f"{node.module}.{alias.name}" if node.module else alias.name
                )
    return result


def expanded(name: str, aliases: dict[str, str]) -> str:
    head, separator, tail = name.partition(".")
    return aliases.get(head, head) + (separator + tail if separator else "")


def sleep_count(tree: ast.AST) -> int:
    aliases = imports(tree)
    count = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or expanded(dotted(node.func), aliases) not in {
            "time.sleep",
            "asyncio.sleep",
        }:
            continue
        argument = (
            node.args[0]
            if node.args
            else next(
                (item.value for item in node.keywords if item.arg in {"delay", "secs", "seconds"}),
                None,
            )
        )
        if isinstance(argument, (ast.Constant, ast.UnaryOp)):
            try:
                if ast.literal_eval(argument) == 0:
                    continue
            except (ValueError, TypeError):
                pass
        count += 1
    return count


def provisioning_call(node: ast.Call) -> bool:
    for argument in [
        ast.Tuple(elts=node.args),
        *node.args,
        *(item.value for item in node.keywords),
    ]:
        if isinstance(argument, (ast.List, ast.Tuple)):
            words = [
                part.value
                if isinstance(part, ast.Constant) and isinstance(part.value, str)
                else "?"
                for part in argument.elts
            ]
        elif isinstance(argument, ast.Constant) and isinstance(argument.value, str):
            words = argument.value.split()
        else:
            continue
        command = " " + " ".join(words) + " "
        if " kind create cluster " in command or " cluster up " in command:
            return True
        if " helm install " in command or (
            " helm upgrade " in command and " --install " in command
        ):
            return True
    return False


def scan(root: Path) -> tuple[dict[str, int], list[str]]:
    aliases: dict[str, dict[str, str]] = {}
    functions: dict[str, tuple[str, list[str]]] = {}
    fixtures: set[str] = set()
    costly: set[str] = set()
    sleeps: dict[str, int] = {}
    for path in python_files(root):
        relative = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text(), filename=relative)
        aliases[relative] = imports(tree)
        test_module = is_test(path) and (
            "tests" in Path(relative).parts
            or path.name == "conftest.py"
            or any(
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name.startswith("test")
                for node in ast.walk(tree)
            )
        )
        if test_module and (count := sleep_count(tree)):
            sleeps[relative] = count
        for statement in tree.body:
            members = [("", statement)]
            if isinstance(statement, ast.ClassDef):
                members = [(statement.name, child) for child in statement.body]
            for owner, function in members:
                if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                name = f"{owner}.{function.name}" if owner else function.name
                key = f"{relative}::{name}"
                calls: list[str] = []
                functions[key] = (relative, calls)
                for decorator in function.decorator_list:
                    target = decorator.func if isinstance(decorator, ast.Call) else decorator
                    if expanded(dotted(target), aliases[relative]) not in {
                        "pytest.fixture",
                        "pytest_asyncio.fixture",
                    }:
                        continue
                    scope = (
                        next((kw.value for kw in decorator.keywords if kw.arg == "scope"), None)
                        if isinstance(decorator, ast.Call)
                        else None
                    )
                    if not isinstance(scope, ast.Constant) or scope.value not in {
                        "session",
                        "module",
                    }:
                        fixtures.add(key)
                instances = {"self": owner, "cls": owner} if owner else {}
                for node in ast.walk(function):
                    if isinstance(node, (ast.Assign, ast.AnnAssign)) and isinstance(
                        node.value, ast.Call
                    ):
                        assignments = (
                            node.targets if isinstance(node, ast.Assign) else [node.target]
                        )
                        for target in assignments:
                            if isinstance(target, ast.Name):
                                instances[target.id] = dotted(node.value.func)
                    if isinstance(node, (ast.List, ast.Tuple)) and provisioning_call(
                        ast.Call(func=ast.Name(id="command"), args=[node], keywords=[])
                    ):
                        costly.add(key)
                    if not isinstance(node, ast.Call):
                        continue
                    if provisioning_call(node):
                        costly.add(key)
                    call = dotted(node.func)
                    head, separator, tail = call.partition(".")
                    if head in instances and separator:
                        call = instances[head] + "." + tail
                    calls.extend((call, call + ".__init__"))
                if key in fixtures:
                    calls.extend(arg.arg for arg in function.args.args + function.args.kwonlyargs)

    symbols: dict[str, set[str]] = {}
    for key, (relative, _) in functions.items():
        parts = relative.removesuffix(".py").replace("/", ".").split(".")
        member = key.split("::", 1)[1]
        for offset in range(len(parts)):
            symbols.setdefault(".".join(parts[offset:]) + "." + member, set()).add(key)

    edges: dict[str, set[str]] = {}
    for key, (relative, calls) in functions.items():
        targets: set[str] = set()
        for name in calls:
            local = f"{relative}::{name}"
            if local in functions:
                targets.add(local)
            else:
                targets.update(symbols.get(expanded(name, aliases[relative]), set()))
        edges[key] = targets
    while True:
        more = {key for key, targets in edges.items() if targets & costly} - costly
        if not more:
            break
        costly.update(more)
    return sleeps, sorted(fixtures & costly)


def duration_entries(paths: Sequence[Path]) -> list[str]:
    entries = set()
    observed = 0
    empty_observation = False
    pattern = re.compile(r"([0-9]+(?:\.[0-9]+)?)s call\s+(.+?)(?:\x1b\[.*)?$")
    for path in paths:
        for line in path.read_text().splitlines():
            empty_observation |= "test-time duration observations: 0 over budget" in line
            match = pattern.search(line)
            if match:
                observed += 1
                if float(match[1]) > BUDGET_SECONDS:
                    entries.add(canonical_nodeid(match[2].strip()))
    if not observed and not empty_observation:
        raise ValueError("duration artifacts contain no pytest call-phase observations")
    return sorted(entries)


def growth(current: dict[str, Any], previous: dict[str, Any]) -> list[str]:
    errors = []
    for path, count in current["sleeps"].items():
        if count > previous["sleeps"].get(path, 0):
            errors.append(f"baseline cannot grow: sleeps {path}")
    for name in ("fixtures", "durations"):
        errors.extend(
            f"baseline cannot grow: {name} {item}"
            for item in sorted(set(current[name]) - set(previous[name]))
        )
    return errors


def check(root: Path, baseline: dict[str, Any]) -> list[str]:
    sleeps, fixtures = scan(root)
    errors = []
    for name, actual in (("sleeps", sleeps), ("fixtures", fixtures)):
        expected = baseline[name]
        for item in sorted(set(actual) | set(expected)):
            left = actual.get(item, 0) if isinstance(actual, dict) else int(item in actual)
            right = expected.get(item, 0) if isinstance(expected, dict) else int(item in expected)
            if left != right:
                errors.append(
                    f"{name}: {'new' if left > right else 'stale'} violation {item} "
                    f"(actual {left}, baseline {right})"
                )
    measured_files: dict[str, set[str]] = {}
    for nodeid in baseline["durations"]:
        file, _, identity = nodeid.partition("::")
        if file not in measured_files:
            path = root / file
            names: set[str] = set()
            if path.is_file():
                tree = ast.parse(path.read_text(), filename=file)
                for statement in tree.body:
                    if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        names.add(statement.name)
                    if isinstance(statement, ast.ClassDef):
                        names.update(
                            statement.name + "::" + member.name
                            for member in statement.body
                            if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef))
                        )
            measured_files[file] = names
        if identity.split("[", 1)[0] not in measured_files[file]:
            errors.append(f"durations: stale test identity {nodeid}")
    return errors


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--regenerate", action="store_true")
    parser.add_argument("--durations", type=Path, action="append", default=[])
    parser.add_argument("--base-ref", default="origin/" + os.environ.get("GITHUB_BASE_REF", "main"))
    args = parser.parse_args(argv)
    root = args.root.resolve()
    path = root / BASELINE
    try:
        if args.regenerate:
            sleeps, fixtures = scan(root)
            previous = read_baseline(path) if path.exists() else None
            if not args.durations and previous is None:
                durations: list[str] = []
            else:
                durations = (
                    duration_entries(args.durations)
                    if args.durations
                    else previous["durations"]
                    if previous is not None
                    else []
                )
            baseline: dict[str, Any] = {
                "sleeps": sleeps,
                "fixtures": fixtures,
                "durations": durations,
            }
            errors = growth(baseline, previous) if previous else []
            if not errors:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n")
        else:
            baseline = read_baseline(path)
            errors = check(root, baseline)
        repository = subprocess.run(
            ["git", "rev-parse", "--git-dir"], cwd=root, capture_output=True
        )
        if repository.returncode == 0:
            reference = subprocess.run(
                ["git", "rev-parse", "--verify", args.base_ref], cwd=root, capture_output=True
            )
            if reference.returncode and not args.regenerate:
                raise ValueError(f"required baseline reference is unavailable: {args.base_ref}")
            listing = subprocess.run(
                ["git", "ls-tree", args.base_ref, "--", BASELINE],
                cwd=root,
                text=True,
                capture_output=True,
                check=False,
            )
            if listing.stdout.strip():
                base = subprocess.run(
                    ["git", "show", f"{args.base_ref}:{BASELINE}"],
                    cwd=root,
                    text=True,
                    capture_output=True,
                    check=True,
                )
                errors.extend(growth(baseline, json.loads(base.stdout)))
        if errors:
            print("\n".join(errors))
            return 1
        print(
            f"Test-time ratchets passed: {sum(baseline['sleeps'].values())} sleeps, "
            f"{len(baseline['fixtures'])} fixtures, "
            f"{len(baseline['durations'])} duration exemptions."
        )
        return 0
    except (ValueError, OSError, SyntaxError, subprocess.CalledProcessError) as error:
        print(f"Test-time ratchets: {error}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

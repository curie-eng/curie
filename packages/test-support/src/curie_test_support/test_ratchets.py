"""No-growth ratchets for test-time costs, compared with the merge-base."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
import sqlite3
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BASELINE = "tests/baselines/test-time.json"
BUDGET_SECONDS = 30
PRUNE_SECONDS = 10
SCANNER_HASH = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def canonical_nodeid(nodeid: str) -> str:
    return nodeid.rsplit("@", 1)[0] if nodeid.rfind("@") > nodeid.rfind("]") else nodeid


def baseline_value(value: Any) -> dict[str, Any]:
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


def read_baseline(path: Path) -> dict[str, Any]:
    return baseline_value(json.loads(path.read_text()))


def base_reference() -> str:
    event = os.environ.get("GITHUB_EVENT_NAME", "")
    if event == "merge_group":
        payload = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
        branch = payload["merge_group"]["base_ref"].removeprefix("refs/heads/")
    elif event in {"pull_request", "pull_request_target"}:
        branch = os.environ.get("GITHUB_BASE_REF", "")
    elif event:
        branch = os.environ.get("GITHUB_REF_NAME", "")
    else:
        branch = os.environ.get("GITHUB_BASE_REF") or "main"
    if not isinstance(branch, str) or not branch:
        raise ValueError(f"base branch is missing for {event}")
    return "origin/" + branch


def ancestor_baseline(root: Path, reference: str | None = None) -> dict[str, Any] | None:
    repository = subprocess.run(["git", "rev-parse", "--git-dir"], cwd=root, capture_output=True)
    if repository.returncode:
        return None
    head = subprocess.run(["git", "rev-parse", "--verify", "HEAD"], cwd=root, capture_output=True)
    if head.returncode:
        return None
    reference = reference or base_reference()
    merge = subprocess.run(
        ["git", "merge-base", "HEAD", reference], cwd=root, text=True, capture_output=True
    )
    if merge.returncode:
        raise ValueError(f"required merge-base is unavailable: {reference}")
    revision = merge.stdout.strip()
    listing = subprocess.run(
        ["git", "ls-tree", revision, "--", BASELINE],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    if not listing.stdout:
        return None
    content = subprocess.run(
        ["git", "show", f"{revision}:{BASELINE}"],
        cwd=root,
        capture_output=True,
        text=True,
        check=True,
    )
    return baseline_value(json.loads(content.stdout))


def python_files(root: Path) -> list[Path]:
    tracked = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", "*.py"],
        cwd=root,
        capture_output=True,
        text=True,
    )
    if tracked.returncode == 0:
        return [
            root / name
            for name in sorted(set(tracked.stdout.splitlines()))
            if (root / name).is_file()
        ]
    return sorted(root.rglob("*.py"))


def dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = dotted(node.value)
        return f"{prefix}.{node.attr}" if prefix else ""
    return ""


def provisions(arguments: Sequence[ast.AST]) -> bool:
    words = [
        item.value if isinstance(item, ast.Constant) and isinstance(item.value, str) else "?"
        for item in arguments
    ]
    command = " " + " ".join(words) + " "
    return (
        " kind create cluster " in command
        or " cluster up " in command
        or " helm install " in command
        or " helm upgrade " in command
        and " --install " in command
    )


class FileScan(ast.NodeVisitor):
    def __init__(self) -> None:
        self.aliases: list[dict[str, str]] = [{}]
        self.prefix = ""
        self.owner = ""
        self.current = ""
        self.functions: dict[str, list[str]] = {}
        self.fixtures: set[str] = set()
        self.costly: set[str] = set()
        self.sleeps = 0
        self.has_test = False

    def expand(self, name: str) -> str:
        head, separator, tail = name.partition(".")
        for scope in reversed(self.aliases):
            if head in scope:
                return scope[head] + (separator + tail if separator else "")
        return name

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.aliases[-1][alias.asname or alias.name.split(".")[0]] = (
                alias.name if alias.asname else alias.name.split(".")[0]
            )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            self.aliases[-1][alias.asname or alias.name] = (
                f"{node.module}.{alias.name}" if node.module else alias.name
            )

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        previous = self.prefix, self.owner
        self.aliases[-1][node.name] = self.prefix + node.name
        self.aliases.append({})
        self.prefix += node.name + "."
        self.owner = self.prefix.removesuffix(".")
        self.generic_visit(node)
        self.prefix, self.owner = previous
        self.aliases.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        name = self.prefix + node.name
        self.aliases[-1][node.name] = name
        self.functions[name] = []
        self.has_test |= node.name.startswith("test")
        for decorator in node.decorator_list:
            target = decorator.func if isinstance(decorator, ast.Call) else decorator
            if self.expand(dotted(target)) not in {"pytest.fixture", "pytest_asyncio.fixture"}:
                continue
            keywords = decorator.keywords if isinstance(decorator, ast.Call) else []
            scope = next((item.value for item in keywords if item.arg == "scope"), None)
            if not isinstance(scope, ast.Constant) or scope.value not in {"session", "module"}:
                self.fixtures.add(name)
                self.functions[name].extend(
                    arg.arg for arg in node.args.args + node.args.kwonlyargs
                )
        previous = self.prefix, self.current
        self.prefix, self.current = name + ".", name
        self.aliases.append({"self": self.owner, "cls": self.owner} if self.owner else {})
        self.generic_visit(node)
        self.aliases.pop()
        self.prefix, self.current = previous

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Assign(self, node: ast.Assign | ast.AnnAssign) -> None:
        value = node.value
        if value is not None:
            name = self.expand(dotted(value.func if isinstance(value, ast.Call) else value))
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name) and name:
                    self.aliases[-1][target.id] = name
        self.generic_visit(node)

    visit_AnnAssign = visit_Assign

    def visit_Call(self, node: ast.Call) -> None:
        name = self.expand(dotted(node.func))
        if name in {"time.sleep", "asyncio.sleep", "anyio.sleep"}:
            argument = (
                node.args[0]
                if node.args
                else next(
                    (
                        item.value
                        for item in node.keywords
                        if item.arg in {"delay", "secs", "seconds"}
                    ),
                    None,
                )
            )
            zero = False
            if isinstance(argument, (ast.Constant, ast.UnaryOp)):
                try:
                    zero = ast.literal_eval(argument) == 0
                except (ValueError, TypeError):
                    pass
            self.sleeps += int(not zero)
        if self.current:
            self.functions[self.current].extend((name, name + ".__init__"))
            if provisions(node.args) or any(
                isinstance(arg, ast.Constant)
                and isinstance(arg.value, str)
                and provisions([ast.Constant(value=word) for word in arg.value.split()])
                for arg in node.args
            ):
                self.costly.add(self.current)
        self.generic_visit(node)

    def visit_List(self, node: ast.List | ast.Tuple) -> None:
        if self.current and provisions(node.elts):
            self.costly.add(self.current)
        self.generic_visit(node)

    visit_Tuple = visit_List

    def visit_Name(self, node: ast.Name) -> None:
        pass

    def visit_Constant(self, node: ast.Constant) -> None:
        pass

    def result(self) -> dict[str, Any]:
        return {
            "functions": self.functions,
            "fixtures": sorted(self.fixtures),
            "costly": sorted(self.costly),
            "sleeps": self.sleeps,
            "has_test": self.has_test,
        }


@dataclass
class Inventory:
    sleeps: dict[str, int] = field(default_factory=dict)
    fixtures: list[str] = field(default_factory=list)
    identities: set[str] = field(default_factory=set)


def scan(root: Path) -> Inventory:
    inventory = Inventory()
    functions: dict[str, tuple[str, list[str]]] = {}
    fixtures: set[str] = set()
    costly: set[str] = set()
    state = (
        Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state")
        / "curie-test-ratchets"
    )
    state.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(state / "scan.sqlite3") as cache:
        cache.execute(
            "CREATE TABLE IF NOT EXISTS scans (digest TEXT PRIMARY KEY, summary TEXT NOT NULL)"
        )
        updates: list[tuple[str, str]] = []
        for path in python_files(root):
            relative = path.relative_to(root).as_posix()
            data = path.read_bytes()
            digest = hashlib.sha256(
                SCANNER_HASH.encode() + relative.encode() + b"\0" + data
            ).hexdigest()
            saved = cache.execute(
                "SELECT summary FROM scans WHERE digest = ?", (digest,)
            ).fetchone()
            if saved:
                summary = json.loads(saved[0])
            else:
                visitor = FileScan()
                visitor.visit(ast.parse(data, filename=relative))
                summary = visitor.result()
                updates.append((digest, json.dumps(summary)))
            test_file = (
                "tests" in Path(relative).parts
                or "test-support" in Path(relative).parts
                or path.name == "conftest.py"
                or (path.name.startswith("test_") or path.name.endswith("_test.py"))
                and summary["has_test"]
            )
            if test_file and summary["sleeps"]:
                inventory.sleeps[relative] = summary["sleeps"]
            for name, calls in summary["functions"].items():
                functions[f"{relative}::{name}"] = relative, calls
                inventory.identities.add(relative + "::" + name.replace(".", "::"))
            fixtures.update(f"{relative}::{name}" for name in summary["fixtures"])
            costly.update(f"{relative}::{name}" for name in summary["costly"])
        cache.executemany("INSERT OR IGNORE INTO scans VALUES (?, ?)", updates)
    symbols: dict[str, set[str]] = {}
    for key, (relative, _) in functions.items():
        parts = relative.removesuffix(".py").replace("/", ".").split(".")
        member = key.split("::", 1)[1]
        for offset in range(len(parts)):
            symbols.setdefault(".".join(parts[offset:]) + "." + member, set()).add(key)
    reverse: dict[str, set[str]] = {}
    for key, (relative, calls) in functions.items():
        for name in calls:
            local = f"{relative}::{name}"
            for target in {local} if local in functions else symbols.get(name, set()):
                reverse.setdefault(target, set()).add(key)
    pending = list(costly)
    while pending:
        for caller in reverse.get(pending.pop(), set()) - costly:
            costly.add(caller)
            pending.append(caller)
    inventory.fixtures = sorted(fixtures & costly)
    return inventory


@dataclass
class Durations:
    maxima: dict[str, float] = field(default_factory=dict)
    collected: dict[str, list[str]] = field(default_factory=dict)


def duration_observations(paths: Sequence[Path]) -> Durations:
    if len({path.resolve() for path in paths}) < 2:
        raise ValueError("duration regeneration requires artifacts from at least two CI runs")
    result = Durations()
    pattern = re.compile(r"([0-9]+(?:\.[0-9]+)?)s call\s+(.+?)$")
    for path in paths:
        observed = False
        for raw in path.read_text().splitlines():
            line = re.sub(r"\x1b\[[0-9;]*m", "", raw)
            if "test-time duration observations:" in line:
                observed = True
            if "test-time collected: " in line:
                result.collected.update(json.loads(line.partition("test-time collected: ")[2]))
            match = pattern.search(line)
            if match:
                observed = True
                nodeid = canonical_nodeid(match[2].strip())
                result.maxima[nodeid] = max(float(match[1]), result.maxima.get(nodeid, 0))
        if not observed:
            raise ValueError("duration artifact contains no call-phase observations")
    return result


def missing_identity(nodeid: str, inventory: Inventory, observations: Durations) -> bool:
    file = nodeid.split("::", 1)[0]
    return (
        nodeid.split("[", 1)[0] not in inventory.identities
        or file in observations.collected
        and nodeid not in observations.collected[file]
    )


def growth(current: dict[str, Any], previous: dict[str, Any]) -> list[str]:
    errors = []
    for path, count in current["sleeps"].items():
        if count > previous["sleeps"].get(path, 0):
            errors.append(f"sleeps: new violation, baseline cannot grow: {path}")
    for name in ("fixtures", "durations"):
        errors.extend(
            f"{name}: new violation, baseline cannot grow: {item}"
            for item in sorted(set(current[name]) - set(previous[name]))
        )
    return errors


def stale_entries(inventory: Inventory, baseline: dict[str, Any]) -> list[str]:
    entries = [
        f"sleeps: stale {path} ({count} -> {inventory.sleeps.get(path, 0)})"
        for path, count in baseline["sleeps"].items()
        if inventory.sleeps.get(path, 0) < count
    ]
    entries.extend(
        f"fixtures: stale {name}"
        for name in sorted(set(baseline["fixtures"]) - set(inventory.fixtures))
    )
    entries.extend(
        f"durations: stale {name}"
        for name in baseline["durations"]
        if missing_identity(name, inventory, Durations())
    )
    return entries


def write_step_summary(title: str, entries: Sequence[str]) -> None:
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary and entries:
        with Path(summary).open("a") as output:
            output.write(title + "\n\n")
            output.write("".join(f"{index}. {item}\n" for index, item in enumerate(entries, 1)))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--regenerate", action="store_true")
    parser.add_argument("--durations", type=Path, action="append", default=[])
    parser.add_argument("--base-ref")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    path = root / BASELINE
    try:
        previous = read_baseline(path) if args.regenerate and path.exists() else None
        if not args.regenerate:
            previous = read_baseline(path)
        ancestor = ancestor_baseline(root, args.base_ref)
        inventory = scan(root)
        observations = duration_observations(args.durations) if args.durations else Durations()
        durations = set(previous["durations"]) if previous else set()
        durations.update(
            name for name, seconds in observations.maxima.items() if seconds > BUDGET_SECONDS
        )
        if args.regenerate:
            durations = {
                name
                for name in durations
                if not missing_identity(name, inventory, observations)
                and observations.maxima.get(name, PRUNE_SECONDS) >= PRUNE_SECONDS
            }
        actual: dict[str, Any] = {
            "sleeps": inventory.sleeps,
            "fixtures": inventory.fixtures,
            "durations": sorted(durations),
        }
        repository = subprocess.run(
            ["git", "rev-parse", "--git-dir"], cwd=root, capture_output=True
        )
        establishing_baseline = args.regenerate and ancestor is None and repository.returncode == 0
        authority = None if establishing_baseline else ancestor or previous
        errors = growth(actual, authority) if authority else []
        if previous and ancestor:
            errors.extend(growth(previous, ancestor))
        if errors:
            print("\n".join(errors))
            return 1
        stale = stale_entries(inventory, previous) if previous else []
        if stale:
            print("\n".join(stale))
            write_step_summary("Stale test-time baseline entries to regenerate:", stale)
        if args.regenerate:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(actual, indent=2, sort_keys=True) + "\n")
        print(
            f"Test-time ratchets passed: {sum(inventory.sleeps.values())} sleeps, "
            f"{len(inventory.fixtures)} fixtures, {len(durations)} duration exemptions."
        )
        return 0
    except (
        ValueError,
        OSError,
        SyntaxError,
        KeyError,
        subprocess.CalledProcessError,
        sqlite3.Error,
    ) as error:
        print(f"Test-time ratchets: {error}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

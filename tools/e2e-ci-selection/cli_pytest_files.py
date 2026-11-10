"""Generate the tracked Python tests that can observe CLI changes."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from collections import Counter
from pathlib import Path, PurePosixPath

REPO_ROOT = Path(__file__).resolve().parents[2]
LIST_PATH = "tools/e2e-ci-selection/cli_pytest_files.txt"
REGENERATE = "uv run python tools/e2e-ci-selection/cli_pytest_files.py --write"
PATTERNS = tuple(
    re.compile(pattern)
    for pattern in (
        r"cli/target",
        r"\bCURIE_BIN\b",
        r"target/(debug|release)/curie\b",
        r"\bcargo\b",
        r"[\"']cli(/|[\"'])",
        r"which\(\s*[\"']curie[\"']\s*\)",
        r"\[\s*[\"']curie[\"']\s*,",
    )
)
HELPER_PREFIXES = (
    "tools",
    "scripts",
    "release",
    "charts/curie/ci",
    "packages/test-support/src",
)


def _under(path: str, prefix: str) -> bool:
    return path.startswith(f"{prefix}/")


def _testpaths(repo_root: Path) -> tuple[str, ...]:
    with (repo_root / "pyproject.toml").open("rb") as stream:
        manifest = tomllib.load(stream)
    try:
        paths = manifest["tool"]["pytest"]["ini_options"]["testpaths"]
    except (KeyError, TypeError) as exc:
        raise ValueError("pyproject.toml must define tool.pytest.ini_options.testpaths") from exc
    if not isinstance(paths, list) or not paths or any(
        not isinstance(path, str)
        or not path
        or "\\" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
        for path in paths
    ):
        raise ValueError("pytest testpaths must be a nonempty list of repository-relative paths")
    return tuple(paths)


def _is_test(path: str, testpaths: tuple[str, ...]) -> bool:
    name = PurePosixPath(path).name
    return (
        path.endswith(".py")
        and (name.startswith("test_") or name.endswith("_test.py"))
        and any(_under(path, prefix) for prefix in testpaths)
    )


def _is_helper(path: str, testpaths: tuple[str, ...]) -> bool:
    return (
        path.endswith((".py", ".sh"))
        and not _is_test(path, testpaths)
        and (
            any(_under(path, prefix) for prefix in HELPER_PREFIXES)
            or path == "get-curie.sh"
            or (path.endswith(".py") and any(_under(path, prefix) for prefix in testpaths))
            or PurePosixPath(path).name == "conftest.py"
        )
    )


def _matches(text: str) -> bool:
    return any(pattern.search(text) for pattern in PATTERNS)


def _links_helper(
    test: PurePosixPath,
    text: str,
    helper: PurePosixPath,
    basenames: Counter[str],
) -> bool:
    if helper.name == "conftest.py" and helper.parent in test.parents:
        return True
    if str(helper) in text or "/".join(helper.parts[-2:]) in text:
        return True
    unique = basenames[helper.name] == 1
    if unique and helper.name in text:
        return True
    if helper.suffix == ".py":
        module = None
        if helper.name == "__init__.py":
            module = helper.parent.name
        elif unique:
            module = helper.stem
        if module is not None:
            name = re.escape(module)
            if re.search(
                rf"\b(?:import\s+{name}\b|from\s+(?:[\w.]*\.)?{name}\s+import\b)", text
            ):
                return True
    return (
        len(helper.parts) >= 3
        and helper.parts[0] == "tools"
        and _under(str(test), f"tools/{helper.parts[1]}/tests")
    )


def generate(repo_root: Path) -> str:
    testpaths = _testpaths(repo_root)
    completed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    tracked = sorted({path for path in completed.stdout.split("\0") if path})
    basenames = Counter(PurePosixPath(path).name for path in tracked)
    tests: dict[str, str] = {}
    helpers: list[PurePosixPath] = []
    for path in tracked:
        if _under(path, "cli"):
            continue
        if _is_test(path, testpaths):
            tests[path] = (repo_root / path).read_text(encoding="utf-8")
        elif _is_helper(path, testpaths):
            if _matches((repo_root / path).read_text(encoding="utf-8")):
                helpers.append(PurePosixPath(path))
    selected = sorted(
        path
        for path, text in tests.items()
        if _matches(text)
        or any(_links_helper(PurePosixPath(path), text, helper, basenames) for helper in helpers)
    )
    return "".join(f"{path}\n" for path in selected)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--write", action="store_true")
    modes.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        expected = generate(REPO_ROOT).encode("utf-8")
        canonical = REPO_ROOT / LIST_PATH
        if args.write:
            canonical.write_bytes(expected)
        elif args.check:
            if canonical.read_bytes() != expected:
                print(
                    f"CLI pytest file list differs. Regenerate with: {REGENERATE}",
                    file=sys.stderr,
                )
                return 1
        else:
            sys.stdout.buffer.write(expected)
    except (OSError, UnicodeError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"CLI pytest file generation failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

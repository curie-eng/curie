"""Release tag fixture for a conflicting advance of the existing PR base."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

CHANGE = "release_tag.py"
PR_TEST = "test_release_tag_pr.py"
BASE_TEST = "test_release_tag_base.py"
VERIFICATION_FILE = ".curie/verification.json"
PROTECTED_PATH = ".github/CODEOWNERS"
VERIFICATION_COMMAND = (
    "python3",
    "-B",
    "-m",
    "unittest",
    "discover",
    "-p",
    "test_release_tag_*.py",
)

_BASELINE_TEST = "test_release_tag_baseline.py"
_COMBINED_TEST = "test_release_tag_combined.py"


def _normalizer(expression: str) -> str:
    return f"def normalize_tag(tag: str) -> str:\n    return {expression}\n"


def _behavior_test(example: str, expected: str) -> str:
    return (
        "import unittest\n"
        "\n"
        "from release_tag import normalize_tag\n"
        "\n\n"
        "class ReleaseTagTest(unittest.TestCase):\n"
        "    def test_normalize_tag(self) -> None:\n"
        f"        self.assertEqual(normalize_tag({example!r}), {expected!r})\n"
        "\n\n"
        'if __name__ == "__main__":\n'
        "    unittest.main()\n"
    )


_BEHAVIOR_TESTS = {
    _BASELINE_TEST: _behavior_test("  stable  ", "stable"),
    PR_TEST: _behavior_test("  Release-Tag  ", "release-tag"),
    BASE_TEST: _behavior_test("  candidate_build  ", "candidate-build"),
    _COMBINED_TEST: _behavior_test("  Daily_Build  ", "daily-build"),
}

_VERIFICATION_DECLARATION = {
    "checks": [
        {
            "id": "release_tag_tests",
            "paths": [CHANGE, "test_release_tag_*.py"],
            "command": list(VERIFICATION_COMMAND),
        }
    ]
}

BASELINE_FILES = {
    CHANGE: _normalizer("tag.strip()"),
    _BASELINE_TEST: _BEHAVIOR_TESTS[_BASELINE_TEST],
    VERIFICATION_FILE: json.dumps(_VERIFICATION_DECLARATION, indent=2) + "\n",
}
INITIAL_PR_FILES = {
    CHANGE: _normalizer("tag.strip().lower()"),
    PR_TEST: _BEHAVIOR_TESTS[PR_TEST],
}
BASE_ADVANCE_FILES = {
    CHANGE: _normalizer('tag.strip().replace("_", "-")'),
    BASE_TEST: _BEHAVIOR_TESTS[BASE_TEST],
}
INCOMPATIBLE_BASE_FILES = {
    CHANGE: BASE_ADVANCE_FILES[CHANGE],
    BASE_TEST: _behavior_test("  Release-Tag  ", "Release-Tag"),
}
RESOLVED_FILES = {CHANGE: _normalizer('tag.strip().lower().replace("_", "-")')}
PROTECTED_BASE_ADVANCE_FILES = {
    **BASE_ADVANCE_FILES,
    PROTECTED_PATH: "# Example ownership policy\n",
}
PROTECTED_REPAIR_FILES = {
    **RESOLVED_FILES,
    PROTECTED_PATH: "# Changed ownership policy\n",
}


def advance_conflicting_base(
    commit_branch: Callable[[str, dict[str, str]], str],
    original_pull: Mapping[str, Any],
    *,
    files: dict[str, str] | None = None,
) -> str:
    """Advance the PR's existing target using the forge fixture's branch writer."""
    base_ref = original_pull["base"]["ref"]
    return commit_branch(base_ref, BASE_ADVANCE_FILES if files is None else files)


def run_behavior_check(checkout: Path, test_file: str) -> subprocess.CompletedProcess[str]:
    """Run the fixture's own test against the code in the given checkout."""
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            "import sys\n"
            "sys.path.insert(0, sys.argv[1])\n"
            "filename = sys.argv[2]\n"
            "del sys.argv[1:]\n"
            "exec(compile(sys.stdin.read(), filename, 'exec'))\n",
            str(checkout.resolve()),
            test_file,
        ],
        cwd=checkout,
        env={"PATH": os.environ.get("PATH", os.defpath)},
        input=_BEHAVIOR_TESTS[test_file],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def assert_resolved_behavior(checkout: Path) -> None:
    """Verify the two changes also work together on a single input."""
    for test_file in _BEHAVIOR_TESTS:
        result = run_behavior_check(checkout, test_file)
        assert result.returncode == 0, f"{test_file} failed on the repair\n{result.stderr}"


def run_declared_verification(checkout: Path) -> subprocess.CompletedProcess[str]:
    """Run the fixture's declared validation with fixture-owned argv."""
    declaration = json.loads((checkout / VERIFICATION_FILE).read_text(encoding="utf-8"))
    assert declaration == _VERIFICATION_DECLARATION, "fixture verification declaration changed"
    return subprocess.run(
        VERIFICATION_COMMAND,
        cwd=checkout,
        env={"PATH": os.environ.get("PATH", os.defpath)},
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def _git(repository: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ("git", "-C", str(repository), *args),
        env={
            "PATH": os.environ.get("PATH", os.defpath),
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
        },
        capture_output=True,
        text=True,
        timeout=15,
        check=check,
    )


def _branch_head(repository: Path, branch: str) -> str:
    return _git(
        repository, "rev-parse", "--verify", "--end-of-options", f"refs/heads/{branch}"
    ).stdout.strip()


def assert_published_repair(
    forge_repository: Path,
    checkout: Path,
    *,
    original_pull: Mapping[str, Any],
    current_pull: Mapping[str, Any],
    accepted_base: str,
    admitted_base_head: str,
    expected_final_head: str,
) -> None:
    """Check an open PR from the shared driver using its actual bare repository.

    Pass snapshots from before the base advance and after publication. The
    checkout is a fresh destination owned by the caller's fixture.
    """
    assert original_pull["state"] == current_pull["state"] == "open", "PR is no longer open"
    for field in ("id", "number"):
        assert original_pull[field] == current_pull[field], "publication changed PR identity"
    assert original_pull["base"]["ref"] == current_pull["base"]["ref"] == accepted_base, (
        "publication changed the accepted named base"
    )
    branch = original_pull["head"]["ref"]
    assert current_pull["head"]["ref"] == branch, "publication changed the PR branch"
    old_head = original_pull["head"]["sha"]
    assert expected_final_head != old_head, "publication still reports the original head"
    assert current_pull["head"]["sha"] == expected_final_head, "PR reports a different final head"
    assert _branch_head(forge_repository, branch) == expected_final_head, (
        "the forge PR branch does not point to the expected final head"
    )
    current_base_head = _branch_head(forge_repository, accepted_base)
    assert current_pull["base"]["sha"] == current_base_head, "the forge base observation is stale"
    original_base_head = original_pull["base"]["sha"]
    assert original_base_head != admitted_base_head, "the named base did not advance"
    for ancestor, descendant in (
        (original_base_head, admitted_base_head),
        (admitted_base_head, current_base_head),
    ):
        ancestry = _git(
            forge_repository, "merge-base", "--is-ancestor", ancestor, descendant, check=False
        )
        assert ancestry.returncode == 0, "the admitted commit is outside the named base's history"

    parents = _git(forge_repository, "rev-list", "--parents", "-1", expected_final_head)
    assert parents.stdout.split() == [expected_final_head, old_head, admitted_base_head], (
        "the final repair does not have the exact parents from admission in order"
    )
    _git(
        checkout.parent,
        "clone",
        "--no-checkout",
        "--no-local",
        "--template=",
        "--",
        str(forge_repository.resolve()),
        str(checkout.resolve()),
    )
    _git(checkout, "switch", "--detach", expected_final_head)
    assert _git(checkout, "rev-parse", "HEAD").stdout.strip() == expected_final_head
    for source_head, test_file in (
        (original_base_head, _BASELINE_TEST),
        (old_head, PR_TEST),
        (admitted_base_head, BASE_TEST),
    ):
        source = _git(forge_repository, "show", f"{source_head}:{test_file}", check=False)
        assert source.returncode == 0 and source.stdout == _BEHAVIOR_TESTS[test_file], (
            f"the source commit did not contain the expected {test_file}"
        )
        published_test = checkout / test_file
        assert (
            published_test.is_file()
            and not published_test.is_symlink()
            and published_test.read_bytes() == _BEHAVIOR_TESTS[test_file].encode()
        ), f"the final repair lost or changed {test_file}"

    assert_resolved_behavior(checkout)
    declared = run_declared_verification(checkout)
    assert declared.returncode == 0, (
        f"declared verification failed on the repair\n{declared.stderr}"
    )
    assert "Ran 3 tests" in declared.stderr, declared.stderr
    assert _branch_head(forge_repository, branch) == expected_final_head, (
        "the forge PR branch changed while verifying the final head"
    )

"""Service tiers for pytest shards: classification, tier-aware sharding, skip guard.

A pull request shard boots only the services its tests need. Three pieces make
that safe: `pytest_impact.tiers_for` classifies each test file statically, the
root conftest's `--ci-shard-tiers STORAGE,FULL` keeps every group that needs a
service on a shard that boots it, and `CURIE_CI_STACK_TIER` turns a skip caused
by a service the shard did not boot into a failure, so a misclassification
cannot silently drop coverage.
"""

from __future__ import annotations

import importlib.util
import os
import re
import shutil
import subprocess
import sys
from functools import cache
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
PYTEST_IMPACT = REPO_ROOT / "tools" / "e2e-ci-selection" / "pytest_impact.py"
ROOT_CONFTEST = REPO_ROOT / "conftest.py"
SUBPROCESS_TIMEOUT = 300

PLAIN_TEST = "def test_add() -> None:\n    assert 1 + 1 == 2\n"
# The real root conftest names Langfuse test files in string constants. It is an
# everything trigger for selection and must never raise a file's service tier.
ROOT_CONFTEST_TEXT = 'SHARED = frozenset({"apps/api/tests/test_langfuse_integration.py"})\n'
ROOT_PYPROJECT_TEXT = (
    "[tool.uv.workspace]\n"
    "members = []\n"
    "\n"
    "[tool.pytest.ini_options]\n"
    'testpaths = ["pkg/tests", "svc/tests", "other/tests"]\n'
)


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None, path
    module = importlib.util.module_from_spec(spec)
    # Dataclasses resolve their module through sys.modules while the class body runs.
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


@pytest.fixture(scope="module")
def impact() -> ModuleType:
    return _load("pytest_impact_under_test", PYTEST_IMPACT)


@pytest.fixture(scope="module")
def root_conftest() -> ModuleType:
    return _load("curie_root_conftest_under_test", ROOT_CONFTEST)


def _write_tree(root: Path, files: dict[str, str]) -> None:
    contents = {"pyproject.toml": ROOT_PYPROJECT_TEXT, "conftest.py": ROOT_CONFTEST_TEXT}
    contents.update(files)
    for relative, text in contents.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    # A tracked-file view is part of the helper resolution contract, so the
    # synthetic tree is a git repository with every file in the index.
    git = shutil.which("git")
    assert git is not None, "git is required to build a synthetic repository"
    for command in (["init", "-q"], ["add", "-A"]):
        subprocess.run(
            [git, *command],
            cwd=root,
            check=True,
            capture_output=True,
            timeout=60,
        )


# Service tier classification


_TIER_CASES = [
    pytest.param(
        {
            "pkg/tests/test_web.py": (
                "import httpx\n\n\n"
                "def test_web() -> None:\n"
                '    httpx.get("http://localhost:23000/api/public/health")\n'
            ),
        },
        "pkg/tests/test_web.py",
        2,
        id="langfuse-web-port",
    ),
    pytest.param(
        {
            "pkg/tests/test_client.py": (
                "from langfuse import Langfuse\n\n\n"
                "def test_client() -> None:\n"
                "    assert Langfuse\n"
            ),
        },
        "pkg/tests/test_client.py",
        2,
        id="langfuse-client-import",
    ),
    pytest.param(
        {
            "pkg/tests/test_client_name.py": (
                "def test_client_name() -> None:\n"
                "    client = make_langfuse_client()\n"
                "    assert client\n"
            ),
        },
        "pkg/tests/test_client_name.py",
        2,
        id="langfuse-client-name",
    ),
    pytest.param(
        {
            "pkg/tests/test_s3_env.py": (
                "import os\n\n\n"
                "def test_s3_env() -> None:\n"
                '    assert os.environ.get("TEST_S3_ENDPOINT_URL")\n'
            ),
        },
        "pkg/tests/test_s3_env.py",
        1,
        id="s3-endpoint-env",
    ),
    pytest.param(
        {
            "pkg/tests/test_bundle.py": (
                'ENDPOINT = "http://rustfs:9000"\n\n\n'
                "def test_bundle() -> None:\n"
                "    assert ENDPOINT\n"
            ),
        },
        "pkg/tests/test_bundle.py",
        1,
        id="rustfs-mention",
    ),
    pytest.param(
        {
            "pkg/tests/test_app.py": (
                "from curie_api.main import create_app\n\n\n"
                "def test_app() -> None:\n"
                "    assert create_app\n"
            ),
        },
        "pkg/tests/test_app.py",
        1,
        id="imports-curie-api",
    ),
    pytest.param({"pkg/tests/test_plain.py": PLAIN_TEST}, "pkg/tests/test_plain.py", 0, id="plain"),
    pytest.param(
        {
            "pkg/tests/test_doc.py": (
                '"""Nothing here needs Langfuse, ClickHouse or the otel-collector."""\n\n'
                "# Langfuse listens on http://localhost:23000 in the full stack.\n\n\n"
                "class TestDoc:\n"
                '    """RustFS buckets are described, never used."""\n\n'
                "    def test_doc(self) -> None:\n"
                '        """Langfuse at localhost:23000 is only mentioned here."""\n'
                "        assert True\n"
            ),
        },
        "pkg/tests/test_doc.py",
        0,
        id="docstring-and-comment-mentions",
    ),
    pytest.param(
        {
            "pkg/tests/_lf_support.py": 'LANGFUSE_HOST = "http://localhost:23000"\n',
            "pkg/tests/test_helper.py": (
                "from _lf_support import LANGFUSE_HOST\n\n\n"
                "def test_helper() -> None:\n"
                "    assert LANGFUSE_HOST\n"
            ),
        },
        "pkg/tests/test_helper.py",
        2,
        id="helper-module-carries-tier",
    ),
    pytest.param(
        {
            "pkg/tests/_chain_b.py": 'BUNDLES = "rustfs-bundles"\n',
            "pkg/tests/_chain_a.py": "from _chain_b import BUNDLES\n\nNAME = BUNDLES\n",
            "pkg/tests/test_chain.py": (
                "from _chain_a import NAME\n\n\ndef test_chain() -> None:\n    assert NAME\n"
            ),
        },
        "pkg/tests/test_chain.py",
        1,
        id="transitive-helper-carries-tier",
    ),
]


@pytest.mark.parametrize("files,target,tier", _TIER_CASES)
def test_tiers_for_classifies_synthetic_test_files(
    impact: ModuleType,
    tmp_path: Path,
    files: dict[str, str],
    target: str,
    tier: int,
) -> None:
    _write_tree(tmp_path, files)

    assert impact.tiers_for(tmp_path, [target]) == {target: tier}


def test_ancestor_conftest_service_raises_only_its_descendants(
    impact: ModuleType,
    tmp_path: Path,
) -> None:
    _write_tree(
        tmp_path,
        {
            "svc/conftest.py": (
                "import pytest\n\n\n"
                "@pytest.fixture\n"
                "def clickhouse_url() -> str:\n"
                '    return "http://localhost:28123"\n'
            ),
            "svc/tests/deep/test_plain.py": PLAIN_TEST,
            "other/tests/test_plain.py": PLAIN_TEST,
        },
    )

    assert impact.tiers_for(
        tmp_path,
        ["svc/tests/deep/test_plain.py", "other/tests/test_plain.py"],
    ) == {"svc/tests/deep/test_plain.py": 2, "other/tests/test_plain.py": 0}


def test_tiers_for_matches_known_repository_files(impact: ModuleType) -> None:
    expected = {
        "apps/api/tests/test_langfuse_integration.py": 2,
        "runner/tests/test_otel.py": 2,
        "apps/worker/tests/eval/test_recorder.py": 2,
        "packages/aci-protocol/tests/test_channel_kind.py": 0,
    }

    assert impact.tiers_for(REPO_ROOT, list(expected)) == expected


# Root conftest tier-aware sharding, through real pytest collection runs


SHARD_COUNT = 3
SHARD_FILES = (
    "packages/aci-protocol/tests/test_channel_kind.py",
    "packages/aci-protocol/tests/test_memory_token.py",
    "packages/aci-protocol/tests/test_ndjson.py",
    "packages/aci-protocol/tests/test_conformance.py",
    "apps/api/tests/test_runner_pods.py",
    "apps/api/tests/test_webhook_body_bound.py",
    "runner/tests/test_otel.py",
)


@cache
def _collect_run(arguments: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
            *arguments,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=SUBPROCESS_TIMEOUT,
    )


def _collect(files: tuple[str, ...], *extra: str) -> list[str]:
    completed = _collect_run((*files, *extra))
    assert completed.returncode == 0, completed.stdout + completed.stderr
    return [line for line in completed.stdout.splitlines() if "::" in line]


def _shard_runs(files: tuple[str, ...], *extra: str) -> list[list[str]]:
    return [
        _collect(files, "--ci-shard", f"{index}/{SHARD_COUNT}", *extra)
        for index in range(1, SHARD_COUNT + 1)
    ]


def _group_of(path: str, conftest: ModuleType) -> str:
    if path in conftest.PROTECTED_RUNNER_FILES:
        return str(conftest.PROTECTED_RUNNER_GROUP)
    if path in conftest.SHARED_LIVE_STATE_FILES:
        return str(conftest.SHARED_LIVE_STATE_GROUP)
    return path


def _todays_assignment(group_sizes: dict[str, int], count: int) -> dict[str, int]:
    """The shard assignment before service tiers, pinned verbatim."""
    loads = [0] * count
    owner: dict[str, int] = {}
    for group, size in sorted(group_sizes.items(), key=lambda entry: (-entry[1], entry[0])):
        target = min(range(count), key=lambda shard: (loads[shard], shard))
        loads[target] += size
        owner[group] = target + 1
    return owner


def _file_of(node: str) -> str:
    return node.split("::")[0]


def test_shard_fixture_files_span_every_tier(impact: ModuleType) -> None:
    tiers = impact.tiers_for(REPO_ROOT, SHARD_FILES)

    assert {tiers[path] for path in SHARD_FILES} == {0, 1, 2}, tiers
    assert tiers["runner/tests/test_otel.py"] == 2
    assert all(tiers[path] == 0 for path in SHARD_FILES[:4]), tiers
    assert all(tiers[path] == 1 for path in SHARD_FILES[4:6]), tiers


@pytest.mark.parametrize("limits", [(2, 1), (1, 1), (3, 2)], ids=["2,1", "1,1", "3,2"])
def test_tiered_shards_keep_service_groups_on_booting_shards(
    impact: ModuleType,
    root_conftest: ModuleType,
    limits: tuple[int, int],
) -> None:
    storage, full_limit = limits
    tiers = impact.tiers_for(REPO_ROOT, SHARD_FILES)
    full = _collect(SHARD_FILES)
    shards = _shard_runs(SHARD_FILES, "--ci-shard-tiers", f"{storage},{full_limit}")

    union = [node for shard in shards for node in shard]
    assert len(union) == len(set(union)), "a node ran in two shards"
    assert sorted(union) == sorted(full), "tiered shards must partition the collection"
    assert all(shards), "every shard must run something at this size"
    group_tier: dict[str, int] = {}
    for node in full:
        group = _group_of(_file_of(node), root_conftest)
        group_tier[group] = max(group_tier.get(group, 0), tiers[_file_of(node)])
    owners: dict[str, set[int]] = {}
    for index, shard in enumerate(shards, start=1):
        for node in shard:
            group = _group_of(_file_of(node), root_conftest)
            owners.setdefault(group, set()).add(index)
            if group_tier[group] == 2:
                assert index <= full_limit, f"{node} needs the full stack, landed on {index}"
            if group_tier[group] >= 1:
                assert index <= storage, f"{node} needs storage, landed on {index}"
    straddling = {group: owned for group, owned in owners.items() if len(owned) > 1}
    assert not straddling, f"groups straddled shards: {straddling}"


@pytest.mark.parametrize(
    "extra",
    [(), ("--ci-shard-tiers", f"{SHARD_COUNT},{SHARD_COUNT}")],
    ids=["no-tiers", "N,N"],
)
def test_untiered_or_unlimited_shards_match_todays_assignment(
    root_conftest: ModuleType,
    extra: tuple[str, ...],
) -> None:
    full = _collect(SHARD_FILES)
    sizes: dict[str, int] = {}
    for node in full:
        group = _group_of(_file_of(node), root_conftest)
        sizes[group] = sizes.get(group, 0) + 1
    owner = _todays_assignment(sizes, SHARD_COUNT)
    expected = [
        [node for node in full if owner[_group_of(_file_of(node), root_conftest)] == index]
        for index in range(1, SHARD_COUNT + 1)
    ]

    assert _shard_runs(SHARD_FILES, *extra) == expected


@pytest.mark.parametrize(
    "files,limits,tier_pattern",
    [
        pytest.param(
            (SHARD_FILES[0], "runner/tests/test_otel.py"),
            f"{SHARD_COUNT},0",
            r"\bfull\b|tier 2",
            id="full-group-without-full-shard",
        ),
        pytest.param(
            (SHARD_FILES[0], "apps/api/tests/test_runner_pods.py"),
            "0,0",
            r"\bstorage\b|tier 1",
            id="storage-group-without-storage-shard",
        ),
    ],
)
def test_service_group_without_a_booting_shard_is_a_usage_error(
    files: tuple[str, ...],
    limits: str,
    tier_pattern: str,
) -> None:
    completed = _collect_run((*files, "--ci-shard", f"1/{SHARD_COUNT}", "--ci-shard-tiers", limits))
    output = completed.stdout + completed.stderr

    assert completed.returncode == pytest.ExitCode.USAGE_ERROR, output
    assert re.search(tier_pattern, output), output


# Skip guard: a skip caused by a service the shard did not boot is a failure


_GUARD_RUNNER = """
import importlib.util
import sys

import pytest

spec = importlib.util.spec_from_file_location("curie_root_conftest_guard", sys.argv[1])
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
raise SystemExit(pytest.main(sys.argv[2:], plugins=[module]))
"""

RUSTFS_REASON = "RustFS bundle bucket not reachable"
LANGFUSE_REASON = "Langfuse not reachable"
UNRELATED_REASON = "requires the real claude CLI on PATH"


def _run_guarded(
    tmp_path: Path,
    files: dict[str, str],
    tier: str | None,
) -> tuple[int, str]:
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    for name, text in files.items():
        (tmp_path / name).write_text(text)
    environment = {key: value for key, value in os.environ.items() if key != "CURIE_CI_STACK_TIER"}
    if tier is not None:
        environment["CURIE_CI_STACK_TIER"] = tier
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            _GUARD_RUNNER,
            str(ROOT_CONFTEST),
            "-p",
            "no:cacheprovider",
            "-rfEs",
            *sorted(files),
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=SUBPROCESS_TIMEOUT,
    )
    return completed.returncode, completed.stdout.replace(str(tmp_path), "<tmp>")


def _runtime_skip(reason: str) -> dict[str, str]:
    return {
        "test_guarded.py": (
            f"import pytest\n\n\ndef test_needs_service() -> None:\n    pytest.skip({reason!r})\n"
        ),
    }


def _module_skip(reason: str) -> dict[str, str]:
    return {
        "test_guarded.py": (
            "import pytest\n\n"
            f"pytest.skip({reason!r}, allow_module_level=True)\n\n\n"
            "def test_needs_service() -> None:\n"
            "    assert True\n"
        ),
        "test_neighbor.py": PLAIN_TEST,
    }


def _failure_text(output: str) -> str:
    markers = [output.find(marker) for marker in ("= FAILURES =", "= ERRORS =")]
    found = [index for index in markers if index >= 0]
    assert found, output
    return output[min(found) :]


@pytest.mark.parametrize(
    "tier,reason",
    [
        pytest.param("core", RUSTFS_REASON, id="core-rustfs"),
        pytest.param("core", LANGFUSE_REASON, id="core-langfuse"),
        pytest.param("storage", LANGFUSE_REASON, id="storage-langfuse"),
    ],
)
def test_skip_guard_fails_a_skip_for_a_service_the_tier_did_not_boot(
    tmp_path: Path,
    tier: str,
    reason: str,
) -> None:
    code, output = _run_guarded(tmp_path, _runtime_skip(reason), tier)

    assert code == pytest.ExitCode.TESTS_FAILED, output
    assert "1 failed" in output, output
    assert tier in _failure_text(output), output


@pytest.mark.parametrize(
    "tier,reason",
    [
        pytest.param("full", RUSTFS_REASON, id="full-rustfs"),
        pytest.param("full", LANGFUSE_REASON, id="full-langfuse"),
        pytest.param("storage", RUSTFS_REASON, id="storage-rustfs"),
        pytest.param("core", UNRELATED_REASON, id="core-unrelated"),
        pytest.param("storage", UNRELATED_REASON, id="storage-unrelated"),
        pytest.param(None, RUSTFS_REASON, id="unset-rustfs"),
        pytest.param(None, LANGFUSE_REASON, id="unset-langfuse"),
    ],
)
def test_skip_guard_leaves_skips_the_tier_explains(
    tmp_path: Path,
    tier: str | None,
    reason: str,
) -> None:
    code, output = _run_guarded(tmp_path, _runtime_skip(reason), tier)

    assert code == pytest.ExitCode.OK, output
    assert "1 skipped" in output, output
    assert "failed" not in output, output


def test_skip_guard_fails_a_module_level_langfuse_skip_in_core(tmp_path: Path) -> None:
    code, output = _run_guarded(tmp_path, _module_skip(LANGFUSE_REASON), "core")

    assert code in (pytest.ExitCode.TESTS_FAILED, pytest.ExitCode.INTERRUPTED), output
    assert "core" in _failure_text(output), output


@pytest.mark.parametrize("tier", [None, "full"], ids=["unset", "full"])
def test_skip_guard_leaves_a_module_level_langfuse_skip_when_booted_or_unset(
    tmp_path: Path,
    tier: str | None,
) -> None:
    code, output = _run_guarded(tmp_path, _module_skip(LANGFUSE_REASON), tier)

    assert code == pytest.ExitCode.OK, output
    assert "1 passed, 1 skipped" in output, output

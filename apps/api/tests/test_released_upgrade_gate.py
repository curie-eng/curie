"""The released-upgrade gate must prove the migrated database is READABLE (#2098).

`scripts/check-released-upgrade.py` proves migrations RUN against a released
database. It cannot prove the result loads: #1914 is a row migration 0021
backfilled verbatim out of `agents.slack_channel`, which then failed
`ChannelBinding`'s address rule and made `GET /agents` return 500 for every
agent, not just the one holding it.

Two phases close that gap -- a `seed` before the candidate upgrade and a
`read-back` after it -- and this module is their contract. It is the frozen API
surface the gate and its read-back runner are built to:

* `SelfTestDirection` / `SELF_TEST_DIRECTIONS` -- the pinned direction table,
  which must keep #1706's negative control (a must-FAIL direction) while adding
  a must-PASS one, because a self test that only knows how to fail is inert.
* `SeedAgent` / `SEED_FIXTURE` -- the seeded rows, asserted here against the
  LIVE `_validate_channel_binding`, so a fixture softened to make the gate green
  goes red instead.
* `ColumnInfo` / `_plan_seed_statements` / `_detect_approval_route_era` -- the
  schema-adaptive planner, which must never silently seed nothing and must pick
  the 0021 channel era and the 0034 approval-route era INDEPENDENTLY (#2098),
  while carrying one non-reserved legacy-state sentinel whenever the released
  schema can represent it (#1901).
* `scripts/released_upgrade_readback.py::_collect_failures` -- the pure
  assertion helper, which must not pass vacuously, must name the DATA before it
  names Pydantic, and must reject every way the legacy-state sentinel can stop
  being one shared runner-visible identity.

Most coverage here is pure and stubbed. The resource checks run when their
selected Compose PostgreSQL service is available, and the sibling identity case
uses a real owned Compose service. No test creates a git worktree, runs `uv
sync`, or runs alembic; the full live gate is exercised by its dedicated driver.
"""

from __future__ import annotations

import asyncio
import dataclasses
import importlib.util
import json
import os
import socket
import sys
import uuid
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from curie_api.schemas import _validate_channel_binding

REPO_ROOT = Path(__file__).resolve().parents[3]
GATE_SCRIPT = REPO_ROOT / "scripts" / "check-released-upgrade.py"
READBACK_SCRIPT = REPO_ROOT / "scripts" / "released_upgrade_readback.py"

# Direction 1, retained verbatim from #1706. The markers are the defense against
# a MissingGreenlet (or any other incidental error) masquerading as the expected
# revision collision, so they are quoted here exactly, not paraphrased.
COLLISION_MARKERS = (
    "asyncpg.exceptions.UndefinedTableError",
    'relation "curie.agent_channels" does not exist',
    "0022_approvals_reply_kind.py",
)

RESERVED_STATE_NAMESPACES = frozenset({"memory", "transcript"})


def _load_script(module_name: str, path: Path) -> ModuleType:
    """Load a `scripts/` file by path.

    `check-released-upgrade.py` has a hyphen and is not importable by name, so
    both modules load the same way, following `test_alembic_revision_gate.py`'s
    `REPO_ROOT` anchor. A missing file fails this one test loudly instead of
    taking the whole collection down with an import error.
    """

    if not path.is_file():
        pytest.fail(f"expected {path} to exist")
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        pytest.fail(f"could not build an import spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def gate() -> ModuleType:
    return _load_script("check_released_upgrade", GATE_SCRIPT)


@pytest.fixture(scope="module")
def readback() -> ModuleType:
    return _load_script("released_upgrade_readback", READBACK_SCRIPT)


_RELEASED_UPGRADE_ENV = (
    "CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT",
    "CURIE_RELEASED_UPGRADE_COMPOSE_FILES",
    "CURIE_RELEASED_UPGRADE_POSTGRES_HOST",
    "CURIE_RELEASED_UPGRADE_POSTGRES_PORT",
)
_CAPTURED_RELEASED_UPGRADE_ENV = {
    name: os.environ.get(name) for name in _RELEASED_UPGRADE_ENV
}
_RELEASED_UPGRADE_INTEGRATION = os.environ.get(
    "CURIE_RELEASED_UPGRADE_INTEGRATION"
)


def _clear_released_upgrade_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _RELEASED_UPGRADE_ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def _clear_isolated_released_upgrade_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_released_upgrade_env(monkeypatch)


@pytest.fixture
def isolated_released_upgrade_resources(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    captured = _CAPTURED_RELEASED_UPGRADE_ENV
    present = [name for name, value in captured.items() if value is not None]
    if present and len(present) != len(captured):
        pytest.fail(
            "isolated PostgreSQL proof captured only some resource settings: "
            f"{', '.join(present)}"
        )
    for name, value in captured.items():
        if value is not None:
            monkeypatch.setenv(name, value)
    resources = gate._released_upgrade_resources_from_env()
    try:
        containers = gate._checked_output(
            gate._compose_command(
                "ps",
                "--all",
                "-q",
                "postgres",
                resources=resources,
            ),
            description="locating selected PostgreSQL for integration proof",
        )
    except gate.GateError:
        if _RELEASED_UPGRADE_INTEGRATION == "1" or os.environ.get("CI"):
            raise
        pytest.skip("selected Compose PostgreSQL is not available")
    if not containers.strip():
        if _RELEASED_UPGRADE_INTEGRATION == "1" or os.environ.get("CI"):
            pytest.fail("selected Compose PostgreSQL is not running")
        pytest.skip("selected Compose PostgreSQL is not running")
    gate._check_postgres(resources=resources)
    return resources


def _database_exists(
    gate: ModuleType,
    database_name: str,
    *,
    resources: Any,
) -> bool:
    output = gate._checked_output(
        gate._database_command(
            "SELECT 1 FROM pg_database WHERE datname = "
            f"'{database_name}'",
            flags=("-t", "-A"),
            resources=resources,
        ),
        description=f"checking owned control database {database_name}",
    )
    return output.strip() == "1"


def _upgrade_database_catalog(
    gate: ModuleType,
    *,
    resources: Any,
) -> tuple[str, ...]:
    prefix = f"curie_upgrade_{os.getpid()}_%"
    output = gate._checked_output(
        gate._database_command(
            "SELECT datname FROM pg_database "
            f"WHERE datname LIKE '{prefix}' ORDER BY datname",
            flags=("-t", "-A"),
            resources=resources,
        ),
        description="reading owned released upgrade database catalog",
    )
    return tuple(name for name in output.splitlines() if name)


@pytest.fixture
def sibling_postgres(
    gate: ModuleType,
    isolated_released_upgrade_resources: Any,
    tmp_path: Path,
) -> Any:
    override = tmp_path / "sibling.yaml"
    override.write_text(
        "services:\n"
        "  postgres:\n"
        "    ports: !override [\"127.0.0.1::5432\"]\n",
        encoding="utf-8",
    )
    project = f"curie2751sibling{uuid.uuid4().hex[:8]}"
    scope = gate.ReleaseUpgradeResources(
        compose_project=project,
        compose_files=(gate.COMPOSE_FILE, override),
        postgres_host="127.0.0.1",
        postgres_port=1,
    )
    try:
        gate._checked_output(
            gate._compose_command(
                "--profile",
                "full",
                "up",
                "-d",
                "--wait",
                "--wait-timeout",
                "120",
                "postgres",
                resources=scope,
            ),
            description="starting owned sibling PostgreSQL",
        )
        endpoints = gate._published_postgres_endpoints(resources=scope)
        assert len(endpoints) == 1
        host, port = endpoints[0]
        assert host == "127.0.0.1"
        sibling = dataclasses.replace(scope, postgres_port=port)
        gate._check_postgres(resources=sibling)
        yield sibling
    finally:
        down = gate._run(
            gate._compose_command(
                "--profile",
                "full",
                "down",
                "-v",
                resources=scope,
            )
        )
        if down.returncode != 0:
            pytest.fail(
                "owned sibling PostgreSQL teardown failed: "
                f"{down.output.strip()}"
            )
        remaining = gate._checked_output(
            gate._compose_command("ps", "--all", "-q", resources=scope),
            description="checking owned sibling PostgreSQL teardown",
        )
        if remaining.strip():
            pytest.fail("owned sibling PostgreSQL containers remain after teardown")
        for resource_kind in ("network", "volume"):
            leftovers = gate._checked_output(
                [
                    "docker",
                    resource_kind,
                    "ls",
                    "--filter",
                    f"label=com.docker.compose.project={project}",
                    "-q",
                ],
                description=f"checking owned sibling {resource_kind} teardown",
            )
            if leftovers.strip():
                pytest.fail(
                    f"owned sibling PostgreSQL {resource_kind} remains after teardown"
                )


# --------------------------------------------------------------------------
# T0: isolated released upgrade resources
# --------------------------------------------------------------------------


def test_released_upgrade_resources_default_to_the_shared_baseline(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_released_upgrade_env(monkeypatch)
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "curieambient")

    resources = gate._released_upgrade_resources_from_env()

    assert resources.compose_project == "curieambient"
    assert resources.compose_files == (gate.COMPOSE_FILE,)
    assert resources.postgres_host == "localhost"
    assert resources.postgres_port == 25432


def test_released_upgrade_resources_keep_the_project_files_and_endpoint_together(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _clear_released_upgrade_env(monkeypatch)
    override = tmp_path / "private.yaml"
    override.write_text("services: {}\n", encoding="utf-8")
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT", "curie2751")
    monkeypatch.setenv(
        "CURIE_RELEASED_UPGRADE_COMPOSE_FILES",
        os.pathsep.join((str(gate.COMPOSE_FILE), str(override))),
    )
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_POSTGRES_PORT", "35432")

    resources = gate._released_upgrade_resources_from_env()

    assert resources.compose_project == "curie2751"
    assert resources.compose_files == (gate.COMPOSE_FILE, override)
    assert resources.postgres_host == "127.0.0.1"
    assert resources.postgres_port == 35432
    assert gate._database_url("curie_upgrade_isolated", resources=resources) == (
        "postgresql+asyncpg://postgres:postgres@127.0.0.1:35432/"
        "curie_upgrade_isolated"
    )


@pytest.mark.parametrize("compose_files_kind", ("base", "override"))
def test_isolated_released_upgrade_resources_require_base_then_override(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    compose_files_kind: str,
) -> None:
    _clear_released_upgrade_env(monkeypatch)
    override = tmp_path / "private.yaml"
    override.write_text("services: {}\n", encoding="utf-8")
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT", "curie2751")
    monkeypatch.setenv(
        "CURIE_RELEASED_UPGRADE_COMPOSE_FILES",
        str(gate.COMPOSE_FILE if compose_files_kind == "base" else override),
    )
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_POSTGRES_PORT", "35432")

    with pytest.raises(gate.GateError, match="COMPOSE"):
        gate._released_upgrade_resources_from_env()


def test_isolated_resource_command_builder_uses_the_same_compose_scope(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _clear_released_upgrade_env(monkeypatch)
    override = tmp_path / "private.yaml"
    override.write_text("services: {}\n", encoding="utf-8")
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT", "curie2751")
    monkeypatch.setenv(
        "CURIE_RELEASED_UPGRADE_COMPOSE_FILES",
        os.pathsep.join((str(gate.COMPOSE_FILE), str(override))),
    )
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_POSTGRES_PORT", "35432")
    resources = gate._released_upgrade_resources_from_env()

    expected_prefix = [
        "docker",
        "compose",
        "-p",
        "curie2751",
        "-f",
        str(gate.COMPOSE_FILE),
        "-f",
        str(override),
    ]
    assert gate._compose_command("ps", resources=resources) == [
        *expected_prefix,
        "ps",
    ]
    create = gate._database_command(
        'CREATE DATABASE "curie_upgrade_isolated"', resources=resources
    )
    cleanup = gate._database_command(
        'DROP DATABASE IF EXISTS "curie_upgrade_isolated" WITH (FORCE)',
        resources=resources,
    )
    for command in (create, cleanup):
        assert command[: len(expected_prefix)] == expected_prefix
        assert command[len(expected_prefix) : len(expected_prefix) + 4] == [
            "exec",
            "-T",
            "postgres",
            "psql",
        ]
        assert command[-4:-1] == ["-v", "ON_ERROR_STOP=1", "-c"]


def _metadata_resources(gate: ModuleType) -> Any:
    return gate.ReleaseUpgradeResources(
        compose_project="curie2751",
        compose_files=(gate.COMPOSE_FILE,),
        postgres_host="localhost",
        postgres_port=25432,
    )


@pytest.mark.parametrize(
    ("container_ids", "labels", "running", "expected"),
    [
        ("", None, None, "exactly one Postgres container"),
        ("one\ntwo\n", None, None, "exactly one Postgres container"),
        (
            "one\n",
            {"com.docker.compose.project": "other", "com.docker.compose.service": "postgres"},
            None,
            "ownership labels",
        ),
        (
            "one\n",
            {"com.docker.compose.project": "curie2751", "com.docker.compose.service": "valkey"},
            None,
            "ownership labels",
        ),
        (
            "one\n",
            {"com.docker.compose.project": "curie2751", "com.docker.compose.service": "postgres"},
            "false",
            "not running",
        ),
    ],
)
def test_selected_postgres_metadata_rejects_unowned_or_unavailable_containers(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    container_ids: str,
    labels: dict[str, str] | None,
    running: str | None,
    expected: str,
) -> None:
    observed: list[list[str]] = []

    def _metadata_output(
        command: list[str],
        *,
        description: str,
        **_kwargs: Any,
    ) -> str:
        observed.append(command)
        if command[:2] == ["docker", "compose"] and "ps" in command:
            return container_ids
        if command[:3] == ["docker", "inspect", "--format"]:
            if ".Config.Labels" in command[3]:
                assert labels is not None
                return json.dumps(labels)
            if ".State.Running" in command[3]:
                assert running is not None
                return running
        pytest.fail(f"unexpected Docker metadata command: {command}")

    monkeypatch.setattr(gate, "_checked_output", _metadata_output)

    with pytest.raises(gate.GateError, match=expected):
        gate._postgres_container_id(resources=_metadata_resources(gate))

    assert all("psql" not in command for command in observed)


@pytest.mark.parametrize(
    "published",
    ("127.0.0.1", "127.0.0.1:not_a_port", "[::1]", "[::1]:65536"),
)
def test_selected_postgres_metadata_rejects_malformed_published_ports(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    published: str,
) -> None:
    observed: list[list[str]] = []

    def _metadata_output(
        command: list[str],
        *,
        description: str,
        **_kwargs: Any,
    ) -> str:
        observed.append(command)
        assert command[:2] == ["docker", "compose"]
        assert "port" in command
        return published

    monkeypatch.setattr(gate, "_checked_output", _metadata_output)

    with pytest.raises(gate.GateError, match="parse published Postgres endpoint"):
        gate._published_postgres_endpoints(resources=_metadata_resources(gate))

    assert all("psql" not in command for command in observed)


@pytest.mark.parametrize(
    ("published_host", "requested_host", "expected"),
    [
        ("127.0.0.1", "localhost", True),
        ("::1", "localhost", True),
        ("0.0.0.0", "localhost", True),
        ("::", "localhost", True),
        ("::1", "127.0.0.1", False),
        ("127.0.0.1", "::1", False),
        ("0.0.0.0", "::1", False),
        ("::", "::1", True),
    ],
)
def test_published_postgres_address_family_matches_the_requested_host(
    gate: ModuleType,
    published_host: str,
    requested_host: str,
    expected: bool,
) -> None:
    assert (
        gate._published_host_serves_requested_host(published_host, requested_host)
        is expected
    )


def test_run_pair_rejects_unowned_metadata_before_database_or_worktree_mutation(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resources = _metadata_resources(gate)
    observed: list[list[str]] = []
    mutations: list[str] = []

    def _metadata_output(
        command: list[str],
        *,
        description: str,
        **_kwargs: Any,
    ) -> str:
        observed.append(command)
        if command == ["docker", "compose", "version"]:
            return "Docker Compose version"
        if command[:2] == ["docker", "compose"] and "ps" in command:
            return "selected\n"
        if command[:3] == ["docker", "inspect", "--format"]:
            assert ".Config.Labels" in command[3]
            return json.dumps(
                {
                    "com.docker.compose.project": "other",
                    "com.docker.compose.service": "postgres",
                }
            )
        pytest.fail(f"unexpected external metadata command: {command}")

    def _record_reset(*_args: Any, **_kwargs: Any) -> None:
        mutations.append("database")

    def _record_worktree(*_args: Any, **_kwargs: Any) -> None:
        mutations.append("worktree")

    monkeypatch.setattr(gate, "_checked_output", _metadata_output)
    monkeypatch.setattr(gate, "_reset_database", _record_reset)
    monkeypatch.setattr(gate, "_add_worktree", _record_worktree)

    with pytest.raises(gate.GateError, match="ownership labels"):
        gate._run_pair(
            resources=resources,
            released_ref="released",
            released_commit="1" * 40,
            candidate_ref="candidate",
            candidate_commit="2" * 40,
            current_candidate=False,
        )

    assert mutations == []
    rendered = "\n".join(" ".join(command) for command in observed)
    assert "DROP DATABASE" not in rendered
    assert "CREATE DATABASE" not in rendered
    assert not any(command[:3] == ["git", "worktree", "add"] for command in observed)


def test_isolated_upgrade_resources_reject_mismatch_and_cleanup_only_owned_database(
    gate: ModuleType,
    isolated_released_upgrade_resources: Any,
) -> None:
    resources = isolated_released_upgrade_resources
    gate._check_postgres(resources=resources)
    sentinel = f"curie_upgrade_control_{uuid.uuid4().hex[:12]}"
    scratch = f"curie_upgrade_scratch_{uuid.uuid4().hex[:12]}"
    sentinel_created = False
    scratch_created = False
    try:
        gate._reset_database(sentinel, resources=resources)
        sentinel_created = True
        assert _database_exists(gate, sentinel, resources=resources)

        mismatched_port = 1 if resources.postgres_port != 1 else 2
        mismatched_resources = dataclasses.replace(
            resources,
            postgres_port=mismatched_port,
        )
        with pytest.raises(gate.GateError):
            gate._check_postgres(resources=mismatched_resources)
        assert _database_exists(gate, sentinel, resources=resources)

        gate._reset_database(scratch, resources=resources)
        scratch_created = True
        gate._cleanup(
            [],
            database_name=scratch,
            database_touched=True,
            resources=resources,
        )
        scratch_created = False
        assert _database_exists(gate, sentinel, resources=resources)
        assert not _database_exists(gate, scratch, resources=resources)
    finally:
        if scratch_created:
            gate._cleanup(
                [],
                database_name=scratch,
                database_touched=True,
                resources=resources,
            )
        if sentinel_created:
            gate._cleanup(
                [],
                database_name=sentinel,
                database_touched=True,
                resources=resources,
            )


def test_run_pair_rejects_sibling_postgres_identity_before_database_mutation(
    gate: ModuleType,
    isolated_released_upgrade_resources: Any,
    sibling_postgres: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = dataclasses.replace(
        isolated_released_upgrade_resources,
        postgres_host="localhost",
    )
    selected_identifier = gate._checked_output(
        gate._database_command(
            "SELECT system_identifier FROM pg_control_system()",
            flags=("-t", "-A"),
            resources=selected,
        ),
        description="reading selected PostgreSQL system identifier",
    ).strip()
    sibling_identifier = asyncio.run(
        gate._host_postgres_system_identifier(resources=sibling_postgres)
    )
    assert selected_identifier
    assert sibling_identifier
    assert selected_identifier != sibling_identifier
    selected_catalog_before = _upgrade_database_catalog(
        gate,
        resources=selected,
    )
    sibling_catalog_before = _upgrade_database_catalog(
        gate,
        resources=sibling_postgres,
    )

    original_getaddrinfo = socket.getaddrinfo

    def _redirect_localhost(
        host: str,
        port: int | str,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[tuple[Any, ...]]:
        if host == "localhost" and str(port) == str(selected.postgres_port):
            return original_getaddrinfo(
                sibling_postgres.postgres_host,
                sibling_postgres.postgres_port,
                family,
                type,
                proto,
                flags,
            )
        return original_getaddrinfo(host, port, family, type, proto, flags)

    with monkeypatch.context() as patched:
        patched.setattr(socket, "getaddrinfo", _redirect_localhost)
        assert asyncio.run(
            gate._host_postgres_system_identifier(resources=selected)
        ) == sibling_identifier
        with pytest.raises(gate.GateError, match="not the selected compose"):
            gate._run_pair(
                resources=selected,
                released_ref="released",
                released_commit="1" * 40,
                candidate_ref="candidate",
                candidate_commit="2" * 40,
                current_candidate=False,
            )

    assert _upgrade_database_catalog(gate, resources=selected) == selected_catalog_before
    assert (
        _upgrade_database_catalog(gate, resources=sibling_postgres)
        == sibling_catalog_before
    )


@pytest.mark.parametrize(
    "set_values",
    [
        {"CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT": "curie2751"},
        {"CURIE_RELEASED_UPGRADE_COMPOSE_FILES": "compose.yaml"},
        {"CURIE_RELEASED_UPGRADE_POSTGRES_HOST": "127.0.0.1"},
        {"CURIE_RELEASED_UPGRADE_POSTGRES_PORT": "35432"},
    ],
)
def test_released_upgrade_resources_reject_partial_environment_before_any_gate_phase(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    set_values: dict[str, str],
) -> None:
    _clear_released_upgrade_env(monkeypatch)
    for name, value in set_values.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(gate.GateError, match="all together"):
        gate._released_upgrade_resources_from_env()


def _set_isolated_released_upgrade_env(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    override: Path,
    *,
    compose_project: str = "curie2751",
    compose_files: str | None = None,
    postgres_host: str = "127.0.0.1",
    postgres_port: str = "35432",
) -> None:
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT", compose_project)
    monkeypatch.setenv(
        "CURIE_RELEASED_UPGRADE_COMPOSE_FILES",
        (
            compose_files
            if compose_files is not None
            else os.pathsep.join((str(gate.COMPOSE_FILE), str(override)))
        ),
    )
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_POSTGRES_HOST", postgres_host)
    monkeypatch.setenv("CURIE_RELEASED_UPGRADE_POSTGRES_PORT", postgres_port)


@pytest.mark.parametrize("compose_files", ("", os.pathsep, "compose.yaml"))
def test_released_upgrade_resources_reject_invalid_compose_files(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    compose_files: str,
) -> None:
    _clear_released_upgrade_env(monkeypatch)
    override = tmp_path / "private.yaml"
    override.write_text("services: {}\n", encoding="utf-8")
    _set_isolated_released_upgrade_env(
        gate,
        monkeypatch,
        override,
        compose_files=compose_files,
    )

    with pytest.raises(gate.GateError, match="COMPOSE_FILES"):
        gate._released_upgrade_resources_from_env()


def test_released_upgrade_resources_reject_missing_compose_override(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _clear_released_upgrade_env(monkeypatch)
    missing_override = tmp_path / "missing.yaml"
    _set_isolated_released_upgrade_env(
        gate,
        monkeypatch,
        missing_override,
    )

    with pytest.raises(gate.GateError, match="COMPOSE_FILES"):
        gate._released_upgrade_resources_from_env()


def test_released_upgrade_resources_reject_empty_compose_project(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _clear_released_upgrade_env(monkeypatch)
    override = tmp_path / "private.yaml"
    override.write_text("services: {}\n", encoding="utf-8")
    _set_isolated_released_upgrade_env(
        gate,
        monkeypatch,
        override,
        compose_project="",
    )

    with pytest.raises(gate.GateError, match="COMPOSE_PROJECT"):
        gate._released_upgrade_resources_from_env()


@pytest.mark.parametrize(
    ("postgres_host", "postgres_port", "expected_field"),
    [
        ("database.example.com", "35432", "POSTGRES_HOST"),
        ("127.0.0.1", "not_a_port", "POSTGRES_PORT"),
        ("127.0.0.1", "0", "POSTGRES_PORT"),
        ("127.0.0.1", "65536", "POSTGRES_PORT"),
    ],
)
def test_released_upgrade_resources_reject_invalid_endpoint_values(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    postgres_host: str,
    postgres_port: str,
    expected_field: str,
) -> None:
    _clear_released_upgrade_env(monkeypatch)
    override = tmp_path / "private.yaml"
    override.write_text("services: {}\n", encoding="utf-8")
    _set_isolated_released_upgrade_env(
        gate,
        monkeypatch,
        override,
        postgres_host=postgres_host,
        postgres_port=postgres_port,
    )

    with pytest.raises(gate.GateError, match=expected_field):
        gate._released_upgrade_resources_from_env()


def _direction(
    gate: ModuleType,
    description: str,
    predicate: Callable[[Any], bool],
) -> Any:
    """The single direction matching `predicate`, or a failure naming the count.

    Every caller wants EXACTLY one row, so the count assertion lives here rather
    than at each call site: a table that grew a second matching direction fails
    loudly instead of silently handing back the first match.
    """

    matching = [row for row in gate.SELF_TEST_DIRECTIONS if predicate(row)]
    assert len(matching) == 1, (
        f"expected exactly one direction {description}, got {len(matching)}"
    )
    return matching[0]


# --------------------------------------------------------------------------
# T1 -- the direction table
# --------------------------------------------------------------------------


def test_direction_is_a_frozen_record_of_refs_expectation_and_markers(
    gate: ModuleType,
) -> None:
    """The row shape: two refs, an expected failure phase, and pinned markers."""
    field_names = {field.name for field in dataclasses.fields(gate.SelfTestDirection)}

    assert field_names == {
        "released_ref",
        "candidate_ref",
        "expect_failure_phase",
        "markers",
    }
    assert isinstance(gate.SELF_TEST_DIRECTIONS, tuple)
    with pytest.raises(dataclasses.FrozenInstanceError):
        gate.SELF_TEST_DIRECTIONS[0].released_ref = "v0.0.0"


def test_exactly_one_direction_pins_a_read_back_failure(gate: ModuleType) -> None:
    """#1914's reproduction: v0.6.2 upgrades cleanly to v0.7.3 and still fails."""
    read_back = _direction(
        gate,
        "expecting a read-back failure",
        lambda row: row.expect_failure_phase == "read-back",
    )

    assert read_back.released_ref == "v0.6.2"
    assert read_back.candidate_ref == "v0.7.3"


def test_exactly_one_direction_must_pass_cleanly(gate: ModuleType) -> None:
    """The must-PASS direction. Without it the self test only knows how to fail."""
    passing = _direction(
        gate,
        "that must pass cleanly",
        lambda row: row.expect_failure_phase is None,
    )

    assert passing.released_ref == "v0.6.2"
    assert passing.candidate_ref == "v0.8.0"


def test_the_1706_collision_direction_is_retained_with_all_three_markers(
    gate: ModuleType,
) -> None:
    """#1706's coverage is EXTENDED here, never replaced."""
    direction = _direction(
        gate,
        "with candidate ref v0.7.0-rc.1",
        lambda row: row.candidate_ref == "v0.7.0-rc.1",
    )

    assert direction.released_ref == "v0.6.2"
    assert direction.expect_failure_phase == "candidate upgrade"
    for marker in COLLISION_MARKERS:
        assert marker in direction.markers, f"lost collision marker {marker!r}"


def test_released_state_historical_directions_keep_exact_verdicts_without_0037(
    gate: ModuleType,
) -> None:
    """#1901 extends HEAD without rewriting any pre-0037 self-test verdict."""
    assert tuple(dataclasses.astuple(row) for row in gate.SELF_TEST_DIRECTIONS) == (
        (
            "v0.6.2",
            "v0.7.0-rc.1",
            "candidate upgrade",
            COLLISION_MARKERS,
        ),
        (
            "v0.6.2",
            "v0.7.3",
            "read-back",
            ("C-0a1b2c3d", "is not a Slack channel ID"),
        ),
        ("v0.6.2", "v0.8.0", None, ()),
    )


# --------------------------------------------------------------------------
# T2 -- `_run_self_test` fails loudly on either mismatch shape
# --------------------------------------------------------------------------


def _matching_result(gate: ModuleType, direction: Any) -> Any:
    """The `PairResult` a direction declares it expects."""
    if direction.expect_failure_phase is None:
        return gate.PairResult("read-back", 0, "")
    return gate.PairResult(
        direction.expect_failure_phase, 1, "\n".join(direction.markers)
    )


def _stub_pair_walk(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, Any],
) -> None:
    """Every direction returns what it declared, except the named overrides.

    Keyed on candidate ref, since the released ref is `v0.6.2` for all three.
    """

    results = {
        direction.candidate_ref: _matching_result(gate, direction)
        for direction in gate.SELF_TEST_DIRECTIONS
    }
    results.update(overrides)

    def _fake_resolve_ref(ref: str) -> str:
        return "0" * 40

    def _fake_run_pair(**kwargs: Any) -> Any:
        return results[kwargs["candidate_ref"]]

    monkeypatch.setattr(gate, "_resolve_ref", _fake_resolve_ref)
    monkeypatch.setattr(gate, "_run_pair", _fake_run_pair)


def test_self_test_passes_when_every_direction_matches_its_expectation(
    gate: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Positive control: without it the mismatch tests could pass vacuously."""
    _stub_pair_walk(gate, monkeypatch, {})

    assert gate._run_self_test(
        resources=gate._released_upgrade_resources_from_env()
    ) == 0


def test_an_expected_failure_that_passed_names_that_direction(
    gate: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A gate that silently stopped exercising the read path must not read green."""
    _stub_pair_walk(
        gate, monkeypatch, {"v0.7.3": gate.PairResult("read-back", 0, "")}
    )

    with pytest.raises(gate.GateError) as excinfo:
        gate._run_self_test(resources=gate._released_upgrade_resources_from_env())

    message = str(excinfo.value)
    assert "v0.7.3" in message
    assert "v0.7.0-rc.1" not in message
    assert "v0.8.0" not in message


def test_an_expected_pass_that_failed_names_the_direction_and_the_phase(
    gate: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other half of AC6: both mismatch shapes are loud, not just one."""
    _stub_pair_walk(
        gate,
        monkeypatch,
        {"v0.8.0": gate.PairResult("candidate upgrade", 1, "boom")},
    )

    with pytest.raises(gate.GateError) as excinfo:
        gate._run_self_test(resources=gate._released_upgrade_resources_from_env())

    message = str(excinfo.value)
    assert "v0.8.0" in message
    assert "candidate upgrade" in message
    assert "v0.7.3" not in message
    assert "v0.7.0-rc.1" not in message


def test_a_failure_in_the_right_phase_missing_a_marker_names_the_marker(
    gate: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The marker set is the defense against the RIGHT phase failing WRONG.

    A `MissingGreenlet`, or any incidental error, still lands in the declared
    phase. Only the markers separate "the failure we pinned" from "some failure",
    so a direction that loses one is a mismatch, not a pass.
    """

    partial_output = "\n".join(COLLISION_MARKERS[:2])
    _stub_pair_walk(
        gate,
        monkeypatch,
        {"v0.7.0-rc.1": gate.PairResult("candidate upgrade", 1, partial_output)},
    )

    with pytest.raises(gate.GateError) as excinfo:
        gate._run_self_test(resources=gate._released_upgrade_resources_from_env())

    message = str(excinfo.value)
    assert "0022_approvals_reply_kind.py" in message
    assert "v0.7.3" not in message
    assert "v0.8.0" not in message


# --------------------------------------------------------------------------
# T3 -- the fixture is legacy-shaped, not validator-shaped
# --------------------------------------------------------------------------


def test_the_fixture_carries_both_a_legacy_and_a_valid_neighbour(
    gate: ModuleType,
) -> None:
    """A fixture of only-legacy or only-valid rows cannot prove the property."""
    legacy = [agent for agent in gate.SEED_FIXTURE if agent.legacy]
    valid = [agent for agent in gate.SEED_FIXTURE if not agent.legacy]

    assert len(legacy) >= 2, "both documented legacy families must be seeded"
    assert len(valid) >= 1, "the valid neighbour proves every agent is returned"
    assert len({agent.name for agent in gate.SEED_FIXTURE}) == len(gate.SEED_FIXTURE)
    assert len({agent.address for agent in gate.SEED_FIXTURE}) == len(
        gate.SEED_FIXTURE
    )


def test_every_legacy_fixture_address_is_rejected_by_the_live_validator(
    gate: ModuleType,
) -> None:
    """The AC1 guarantee: shapes the RELEASED code permitted, read from the gate.

    Read from `SEED_FIXTURE` rather than restated as literals on purpose. If
    someone softens an address to make the gate green, the address stops being a
    shape the current write path rejects, and this goes red.
    """

    for agent in gate.SEED_FIXTURE:
        if not agent.legacy:
            continue
        with pytest.raises(ValueError):
            _validate_channel_binding(agent.kind, agent.address)


def test_the_valid_fixture_address_is_accepted_by_the_live_validator(
    gate: ModuleType,
) -> None:
    for agent in gate.SEED_FIXTURE:
        if agent.legacy:
            continue
        assert _validate_channel_binding(agent.kind, agent.address) == agent.address


def test_the_seeded_approval_route_reuses_a_rejected_legacy_address(
    gate: ModuleType,
) -> None:
    """The sibling read projections are only pinned if the route address is legacy."""
    routed = [
        agent
        for agent in gate.SEED_FIXTURE
        if agent.approval_route_address is not None
    ]

    assert len(routed) == 1
    assert routed[0].legacy
    assert routed[0].approval_route_address == routed[0].address


# --------------------------------------------------------------------------
# T4 -- the seed planner branches correctly
# --------------------------------------------------------------------------


def _column(
    gate: ModuleType,
    table_name: str,
    column_name: str,
    *,
    nullable: bool = True,
    default: str | None = None,
) -> Any:
    return gate.ColumnInfo(
        table_name=table_name,
        column_name=column_name,
        is_nullable="YES" if nullable else "NO",
        column_default=default,
    )


def _legacy_columns(gate: ModuleType) -> tuple[Any, ...]:
    """v0.6.2's shape: `agents.slack_channel` NOT NULL, no `agent_channels`."""
    return (
        _column(gate, "agents", "id", nullable=False, default="gen_random_uuid()"),
        _column(gate, "agents", "name", nullable=False),
        _column(gate, "agents", "slack_channel", nullable=False),
        _column(gate, "agents", "created_at", nullable=False, default="now()"),
        _column(gate, "agents", "approval_routes"),
    )


def _agent_channels_columns(gate: ModuleType) -> tuple[Any, ...]:
    """v0.8.0's shape: `slack_channel` gone, `agent_channels` present."""
    return (
        _column(gate, "agents", "id", nullable=False, default="gen_random_uuid()"),
        _column(gate, "agents", "name", nullable=False),
        _column(gate, "agents", "created_at", nullable=False, default="now()"),
        _column(gate, "agents", "approval_routes"),
        _column(
            gate,
            "agent_channels",
            "id",
            nullable=False,
            default="gen_random_uuid()",
        ),
        _column(gate, "agent_channels", "agent_id", nullable=False),
        _column(gate, "agent_channels", "kind", nullable=False),
        _column(gate, "agent_channels", "address", nullable=False),
        _column(gate, "agent_channels", "generation", nullable=False, default="0"),
        _column(gate, "agent_channels", "endpoint"),
        _column(gate, "agent_channels", "adapter"),
    )


def _state_columns(
    gate: ModuleType,
    *,
    binding_scope: bool,
) -> tuple[Any, ...]:
    """The state table before or after 0031 added `binding_scope`."""
    columns = (
        _column(
            gate,
            "workflow_state_entries",
            "id",
            nullable=False,
            default="gen_random_uuid()",
        ),
        _column(
            gate,
            "workflow_state_entries",
            "agent_id",
            nullable=False,
        ),
        _column(
            gate,
            "workflow_state_entries",
            "namespace",
            nullable=False,
        ),
        _column(
            gate,
            "workflow_state_entries",
            "key",
            nullable=False,
        ),
        _column(
            gate,
            "workflow_state_entries",
            "value",
            nullable=False,
        ),
        _column(
            gate,
            "workflow_state_entries",
            "version",
            nullable=False,
            default="1",
        ),
        _column(
            gate,
            "workflow_state_entries",
            "created_at",
            nullable=False,
            default="now()",
        ),
        _column(
            gate,
            "workflow_state_entries",
            "updated_at",
            nullable=False,
            default="now()",
        ),
    )
    if not binding_scope:
        return columns
    return (
        *columns,
        _column(gate, "workflow_state_entries", "binding_scope"),
    )


def _plan_seed(
    gate: ModuleType,
    columns: tuple[Any, ...],
    *,
    approval_route_era: str,
    released_state_repaired: bool = False,
) -> tuple[tuple[str, ...], Any]:
    """The planner's SQL and the exact metadata that describes what it wrote."""
    statements, metadata = gate._plan_seed_statements(
        columns,
        approval_route_era=approval_route_era,
        released_state_repaired=released_state_repaired,
    )
    assert isinstance(statements, tuple)
    return statements, metadata


def _planned_statements(
    gate: ModuleType,
    columns: tuple[Any, ...],
    *,
    approval_route_era: str,
) -> tuple[str, ...]:
    return _plan_seed(
        gate,
        columns,
        approval_route_era=approval_route_era,
    )[0]


def _routed_agent(gate: ModuleType) -> Any:
    """The one fixture row that carries an approval route."""
    return next(
        agent
        for agent in gate.SEED_FIXTURE
        if agent.approval_route_address is not None
    )


def test_legacy_column_branch_writes_every_address_into_curie_agents(
    gate: ModuleType,
) -> None:
    statements = _planned_statements(
        gate,
        _legacy_columns(gate),
        approval_route_era=gate.APPROVAL_ROUTE_ERA_LEGACY,
    )

    rendered = "\n".join(statements)
    assert statements, "the seed must never plan nothing"
    assert "curie.agents" in rendered
    assert "slack_channel" in rendered
    assert "curie.agent_channels" not in rendered
    for agent in gate.SEED_FIXTURE:
        assert agent.name in rendered
        assert agent.address in rendered


def test_agent_channels_branch_writes_agent_id_kind_and_address(
    gate: ModuleType,
) -> None:
    statements = _planned_statements(
        gate,
        _agent_channels_columns(gate),
        approval_route_era=gate.APPROVAL_ROUTE_ERA_SPLIT,
    )

    rendered = "\n".join(statements)
    binding_statements = [
        statement for statement in statements if "curie.agent_channels" in statement
    ]
    assert binding_statements, "the agent_channels branch must write bindings"
    binding_text = "\n".join(binding_statements)
    for column in ("agent_id", "kind", "address"):
        assert column in binding_text
    assert "slack_channel" not in rendered
    for agent in gate.SEED_FIXTURE:
        assert agent.name in rendered
        assert agent.address in binding_text
        assert agent.kind in binding_text


@pytest.mark.parametrize("binding_scope", [False, True])
def test_released_state_planner_adds_one_non_reserved_sentinel_in_each_schema_era(
    gate: ModuleType,
    binding_scope: bool,
) -> None:
    """The released row is old-shaped SQL, not a current-model construction."""
    statements, metadata = _plan_seed(
        gate,
        (
            *_agent_channels_columns(gate),
            *_state_columns(gate, binding_scope=binding_scope),
        ),
        approval_route_era=gate.APPROVAL_ROUTE_ERA_SPLIT,
    )

    state_statements = [
        statement
        for statement in statements
        if "curie.workflow_state_entries" in statement
    ]
    assert len(state_statements) == 1
    state_sql = state_statements[0]
    sentinel = metadata.legacy_state
    assert sentinel is not None
    assert sentinel.owner_name in {agent.name for agent in gate.SEED_FIXTURE}
    assert sentinel.namespace not in RESERVED_STATE_NAMESPACES
    assert sentinel.owner_name in state_sql
    assert sentinel.namespace in state_sql
    assert sentinel.key in state_sql
    assert json.dumps(sentinel.value) in state_sql
    if binding_scope:
        assert "binding_scope" in state_sql
        assert "NULL" in state_sql
    else:
        assert "binding_scope" not in state_sql


@pytest.mark.parametrize("channel_era", ["legacy", "agent_channels"])
def test_released_state_planner_keeps_old_schemas_without_the_table_supported(
    gate: ModuleType,
    channel_era: str,
) -> None:
    columns = (
        _legacy_columns(gate)
        if channel_era == "legacy"
        else _agent_channels_columns(gate)
    )
    route_era = (
        gate.APPROVAL_ROUTE_ERA_LEGACY
        if channel_era == "legacy"
        else gate.APPROVAL_ROUTE_ERA_SPLIT
    )

    statements, metadata = _plan_seed(
        gate,
        columns,
        approval_route_era=route_era,
    )

    assert statements
    assert metadata.legacy_state is None
    assert all("workflow_state_entries" not in statement for statement in statements)


def test_released_state_introspection_includes_the_workflow_state_table(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    def _fake_query(database_name: str, sql: str, *, resources: Any) -> str:
        captured["database_name"] = database_name
        captured["sql"] = sql
        return "workflow_state_entries|namespace|NO|"

    monkeypatch.setattr(gate, "_scratch_query", _fake_query)

    columns = gate._introspect_columns(
        "released_state_introspection",
        resources=gate._released_upgrade_resources_from_env(),
    )

    assert captured["database_name"] == "released_state_introspection"
    assert "workflow_state_entries" in captured["sql"]
    assert columns == (
        gate.ColumnInfo(
            table_name="workflow_state_entries",
            column_name="namespace",
            is_nullable="NO",
            column_default=None,
        ),
    )


def test_legacy_branch_seeds_the_pre_0034_approval_route_shape(
    gate: ModuleType,
) -> None:
    """Pre-0034 storage: `{"deploy": {"channel": ...}}`, rewritten on the way up."""
    rendered = "\n".join(
        _planned_statements(
            gate,
            _legacy_columns(gate),
            approval_route_era=gate.APPROVAL_ROUTE_ERA_LEGACY,
        )
    )

    routed = _routed_agent(gate)
    assert "approval_routes" in rendered
    assert "channel" in rendered
    assert routed.approval_route_address in rendered


def test_agent_channels_branch_seeds_the_post_0034_approval_route_shape(
    gate: ModuleType,
) -> None:
    """Post-0034 storage: the split `{"resolution": {"kind", "address"}}` shape."""
    rendered = "\n".join(
        _planned_statements(
            gate,
            _agent_channels_columns(gate),
            approval_route_era=gate.APPROVAL_ROUTE_ERA_SPLIT,
        )
    )

    routed = _routed_agent(gate)
    assert "approval_routes" in rendered
    assert "resolution" in rendered
    assert routed.approval_route_address in rendered


def _legacy_route_json(gate: ModuleType) -> str:
    """The pre-0034 route literal, built the way the planner builds it."""
    routed = _routed_agent(gate)
    return json.dumps({"deploy": {"channel": routed.approval_route_address}})


def _split_route_json(gate: ModuleType) -> str:
    """The post-0034 route literal, key order included (#1460)."""
    routed = _routed_agent(gate)
    return json.dumps(
        {
            "deploy": {
                "resolution": {
                    "kind": routed.kind,
                    "address": routed.approval_route_address,
                }
            }
        }
    )


def _fake_released_tree(tmp_path: Path, *, revisions: tuple[str, ...]) -> Path:
    """A worktree-shaped directory holding just the named revision files."""
    versions = tmp_path / "apps" / "api" / "alembic" / "versions"
    versions.mkdir(parents=True)
    for name in revisions:
        (versions / name).write_text("# stub revision\n", encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize(
    ("revisions", "expected"),
    [
        (("0036_agents_hook_partitions.py",), False),
        (("0037_multibinding_state_identity.py",), True),
        (("0037.py",), False),
        (("00370_not_the_repair.py",), False),
    ],
)
def test_released_state_candidate_capability_is_pinned_to_an_exact_0037_file(
    gate: ModuleType,
    tmp_path: Path,
    revisions: tuple[str, ...],
    expected: bool,
) -> None:
    tree = _fake_released_tree(tmp_path, revisions=revisions)

    assert gate._candidate_supports_legacy_state(tree) is expected


def _metadata_with_released_state(gate: ModuleType) -> Any:
    return _plan_seed(
        gate,
        (
            *_agent_channels_columns(gate),
            *_state_columns(gate, binding_scope=True),
        ),
        approval_route_era=gate.APPROVAL_ROUTE_ERA_SPLIT,
    )[1]


def _argument_value(command: list[str], option: str) -> str:
    index = command.index(option)
    return command[index + 1]


def test_released_state_readback_arguments_are_enabled_only_by_exact_0037(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    metadata = _metadata_with_released_state(gate)
    sentinel = metadata.legacy_state
    assert sentinel is not None
    captured: list[list[str]] = []

    def _fake_run_in_tree(command: list[str], **_kwargs: Any) -> Any:
        captured.append(command)
        return gate.CommandResult(0, "ok")

    monkeypatch.setattr(gate, "_run_in_tree", _fake_run_in_tree)
    without_repair = _fake_released_tree(
        tmp_path / "without-repair",
        revisions=("0036_agents_hook_partitions.py",),
    )
    with_repair = _fake_released_tree(
        tmp_path / "with-repair",
        revisions=("0037_multibinding_state_identity.py",),
    )

    for candidate_tree in (without_repair, with_repair):
        result = gate._run_readback(
            candidate_tree,
            database_url="postgresql+asyncpg://gate/test",
            phase=gate.READBACK_PHASE,
            ref="candidate",
            commit="0" * 40,
            seed_metadata=metadata,
        )
        assert result.returncode == 0

    state_options = {
        "--expect-state-owner",
        "--expect-state-namespace",
        "--expect-state-key",
        "--expect-state-value",
    }
    assert state_options.isdisjoint(captured[0])
    assert _argument_value(captured[1], "--expect-state-owner") == sentinel.owner_name
    assert (
        _argument_value(captured[1], "--expect-state-namespace")
        == sentinel.namespace
    )
    assert _argument_value(captured[1], "--expect-state-key") == sentinel.key
    assert json.loads(_argument_value(captured[1], "--expect-state-value")) == (
        sentinel.value
    )


def test_released_state_readback_names_pre_table_skip_for_a_0037_candidate(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    metadata = dataclasses.replace(
        _metadata_with_released_state(gate),
        legacy_state=None,
    )
    captured: dict[str, list[str]] = {}

    def _fake_run_in_tree(command: list[str], **_kwargs: Any) -> Any:
        captured["command"] = command
        return gate.CommandResult(0, "ok")

    monkeypatch.setattr(gate, "_run_in_tree", _fake_run_in_tree)
    candidate_tree = _fake_released_tree(
        tmp_path,
        revisions=("0037_multibinding_state_identity.py",),
    )

    result = gate._run_readback(
        candidate_tree,
        database_url="postgresql+asyncpg://gate/test",
        phase=gate.READBACK_PHASE,
        ref="candidate",
        commit="0" * 40,
        seed_metadata=metadata,
    )

    assert result.returncode == 0
    state_options = {
        "--expect-state-owner",
        "--expect-state-namespace",
        "--expect-state-key",
        "--expect-state-value",
    }
    assert state_options.isdisjoint(captured["command"])
    output = capsys.readouterr().out.lower()
    assert "skip" in output
    assert "released schema predates" in output
    assert "workflow_state_entries" in output


def test_seed_released_database_returns_the_exact_released_state_metadata(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = _metadata_with_released_state(gate)
    monkeypatch.setattr(
        gate,
        "_introspect_columns",
        lambda _database, *, resources: (),
    )
    monkeypatch.setattr(
        gate,
        "_plan_seed_statements",
        lambda _columns, *, approval_route_era, released_state_repaired: (
            ("SELECT 1",),
            metadata,
        ),
    )
    monkeypatch.setattr(gate, "_checked_output", lambda *_args, **_kwargs: "")

    returned = gate._seed_released_database(
        "released_state_seed",
        phase=gate.SEED_PHASE,
        approval_route_era=gate.APPROVAL_ROUTE_ERA_SPLIT,
        resources=gate._released_upgrade_resources_from_env(),
    )

    assert returned is metadata


def test_seed_released_database_refuses_state_capable_schema_without_a_sentinel(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    columns = (
        *_agent_channels_columns(gate),
        *_state_columns(gate, binding_scope=True),
    )
    metadata = _metadata_with_released_state(gate)
    empty_metadata = dataclasses.replace(metadata, legacy_state=None)
    monkeypatch.setattr(
        gate,
        "_introspect_columns",
        lambda _database, *, resources: columns,
    )
    monkeypatch.setattr(
        gate,
        "_plan_seed_statements",
        lambda _columns, *, approval_route_era, released_state_repaired: (
            ("SELECT 1",),
            empty_metadata,
        ),
    )

    with pytest.raises(gate.GateError) as excinfo:
        gate._seed_released_database(
            "released_state_vacuity",
            phase=gate.SEED_PHASE,
            approval_route_era=gate.APPROVAL_ROUTE_ERA_SPLIT,
            resources=gate._released_upgrade_resources_from_env(),
        )

    message = str(excinfo.value)
    assert "workflow_state_entries" in message
    assert "sentinel" in message


def test_upgrade_pair_threads_the_exact_released_state_metadata_into_readback(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    metadata = _metadata_with_released_state(gate)
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        gate,
        "_run_alembic",
        lambda *_args, **_kwargs: gate.CommandResult(0, "ok"),
    )
    monkeypatch.setattr(
        gate,
        "_detect_approval_route_era",
        lambda _tree: gate.APPROVAL_ROUTE_ERA_SPLIT,
    )
    monkeypatch.setattr(
        gate,
        "_seed_released_database",
        lambda *_args, **_kwargs: metadata,
    )

    def _fake_readback(*_args: Any, **kwargs: Any) -> Any:
        captured.update(kwargs)
        return gate.CommandResult(0, "read-back ok")

    monkeypatch.setattr(gate, "_run_readback", _fake_readback)

    result = gate._upgrade_pair(
        tmp_path / "released",
        tmp_path / "candidate",
        database_url="postgresql+asyncpg://gate/test",
        database_name="released_state_upgrade",
        released_ref="v0.8.0",
        released_commit="1" * 40,
        candidate_ref="HEAD",
        candidate_commit="2" * 40,
        resources=gate._released_upgrade_resources_from_env(),
    )

    assert result == gate.PairResult(gate.READBACK_PHASE, 0, "read-back ok")
    assert captured["seed_metadata"] is metadata


def test_upgrade_pair_from_released_0037_seeds_a_valid_shared_state_owner(
    gate: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A stable 0037 line must not recreate the pre-repair invalid owner shape.

    The sentinel remains enabled in the candidate read-back so this direction
    still proves exactly one NULL-scope row, unchanged value, and a
    runner-visible memory owner. Only the one-shot false-to-true migration repair
    is no longer fabricated after that migration has already shipped.
    """
    released_tree = _fake_released_tree(
        tmp_path / "released",
        revisions=("0037_multibinding_state_identity.py",),
    )
    candidate_tree = _fake_released_tree(
        tmp_path / "candidate",
        revisions=("0037_multibinding_state_identity.py",),
    )
    columns = (
        *_agent_channels_columns(gate),
        _column(gate, "agents", "memory", nullable=False, default="false"),
        *_state_columns(gate, binding_scope=True),
    )
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        gate,
        "_run_alembic",
        lambda *_args, **_kwargs: gate.CommandResult(0, "ok"),
    )
    monkeypatch.setattr(
        gate,
        "_detect_approval_route_era",
        lambda _tree: gate.APPROVAL_ROUTE_ERA_SPLIT,
    )

    def _fake_seed(*_args: Any, **kwargs: Any) -> Any:
        captured["released_state_repaired"] = kwargs["released_state_repaired"]
        statements, metadata = gate._plan_seed_statements(
            columns,
            approval_route_era=kwargs["approval_route_era"],
            released_state_repaired=kwargs["released_state_repaired"],
        )
        captured["statements"] = statements
        return metadata

    def _fake_run_in_tree(command: list[str], **_kwargs: Any) -> Any:
        captured["readback_command"] = command
        return gate.CommandResult(0, "read-back ok")

    monkeypatch.setattr(gate, "_seed_released_database", _fake_seed)
    monkeypatch.setattr(gate, "_run_in_tree", _fake_run_in_tree)

    result = gate._upgrade_pair(
        released_tree,
        candidate_tree,
        database_url="postgresql+asyncpg://gate/test",
        database_name="released_state_already_repaired",
        released_ref="v0.9.0",
        released_commit="1" * 40,
        candidate_ref="HEAD",
        candidate_commit="2" * 40,
        resources=gate._released_upgrade_resources_from_env(),
    )

    assert result == gate.PairResult(gate.READBACK_PHASE, 0, "read-back ok")
    assert captured["released_state_repaired"] is True
    owner_insert = next(
        statement
        for statement in captured["statements"]
        if gate.LEGACY_STATE_SEED.owner_name in statement
        and "INSERT INTO curie.agents" in statement
    )
    assert "memory" in owner_insert
    assert "TRUE" in owner_insert
    state_insert = next(
        statement
        for statement in captured["statements"]
        if "INSERT INTO curie.workflow_state_entries" in statement
    )
    assert "binding_scope" in state_insert
    assert "NULL" in state_insert
    for option in (
        "--expect-state-owner",
        "--expect-state-namespace",
        "--expect-state-key",
        "--expect-state-value",
    ):
        assert option in captured["readback_command"]


def test_agent_channels_branch_with_a_legacy_route_era_seeds_the_channel_shape(
    gate: ModuleType,
) -> None:
    """The between-eras quadrant: post-0021 bindings, pre-0034 route (#2098).

    A released ref in v0.7.0..v0.7.3 binds channels through `agent_channels` and
    STILL stores `{"deploy": {"channel": ...}}`. Reading the route shape off the
    0021 discriminator seeds `resolution` there -- a future-shaped fixture the
    released code could never have produced -- and the candidate's 0034 (#1460)
    then rejects it as an unknown legacy key, so the gate blames the migration
    for its own seed. Revert the era split and this test goes red.
    """

    rendered = "\n".join(
        _planned_statements(
            gate,
            _agent_channels_columns(gate),
            approval_route_era=gate.APPROVAL_ROUTE_ERA_LEGACY,
        )
    )

    assert "curie.agent_channels" in rendered
    assert _legacy_route_json(gate) in rendered
    assert "resolution" not in rendered


def test_agent_channels_branch_with_a_split_route_era_seeds_the_resolution_shape(
    gate: ModuleType,
) -> None:
    """Post-0034 released refs keep the split shape: the era is a choice, not a drop."""

    rendered = "\n".join(
        _planned_statements(
            gate,
            _agent_channels_columns(gate),
            approval_route_era=gate.APPROVAL_ROUTE_ERA_SPLIT,
        )
    )

    assert _split_route_json(gate) in rendered
    assert _legacy_route_json(gate) not in rendered


def test_legacy_column_branch_with_a_legacy_route_era_seeds_the_channel_shape(
    gate: ModuleType,
) -> None:
    """Pre-0021 implies pre-0034, so this quadrant is unchanged by the era split."""

    rendered = "\n".join(
        _planned_statements(
            gate,
            _legacy_columns(gate),
            approval_route_era=gate.APPROVAL_ROUTE_ERA_LEGACY,
        )
    )

    assert "slack_channel" in rendered
    assert _legacy_route_json(gate) in rendered
    assert "resolution" not in rendered


def test_a_released_tree_holding_0034_is_the_split_era(
    gate: ModuleType,
    tmp_path: Path,
) -> None:
    """The released WORKTREE is the authority: 0034 adds no schema to introspect."""

    tree = _fake_released_tree(
        tmp_path,
        revisions=("0021_agent_channels.py", "0034_approval_route_targets.py"),
    )

    assert gate._detect_approval_route_era(tree) == gate.APPROVAL_ROUTE_ERA_SPLIT


def test_a_released_tree_without_0034_is_the_legacy_era(
    gate: ModuleType,
    tmp_path: Path,
) -> None:
    """v0.7.0..v0.7.3's shape: 0021 has run, 0034 has not."""

    tree = _fake_released_tree(
        tmp_path,
        revisions=("0021_agent_channels.py", "0033_action_gate_approval.py"),
    )

    assert gate._detect_approval_route_era(tree) == gate.APPROVAL_ROUTE_ERA_LEGACY


def test_a_released_tree_with_no_versions_directory_raises(
    gate: ModuleType,
    tmp_path: Path,
) -> None:
    """No versions directory means the tree is not what we think it is.

    Guessing an era from a tree we cannot read is how a future-shaped fixture
    gets seeded in the first place, so refuse instead of defaulting.
    """

    with pytest.raises(gate.GateError) as excinfo:
        gate._detect_approval_route_era(tmp_path)

    assert "alembic versions directory" in str(excinfo.value)


def test_an_unrecognized_approval_route_era_raises_naming_the_value(
    gate: ModuleType,
) -> None:
    """No default and no fallthrough: a bad era must never select a shape."""

    with pytest.raises(gate.GateError) as excinfo:
        gate._plan_seed_statements(
            _agent_channels_columns(gate), approval_route_era="0034"
        )

    message = str(excinfo.value)
    assert "0034" in message
    assert gate.APPROVAL_ROUTE_ERA_LEGACY in message
    assert gate.APPROVAL_ROUTE_ERA_SPLIT in message


def test_neither_binding_location_raises_naming_both_candidates(
    gate: ModuleType,
) -> None:
    """A seed that no-ops turns the read-back into a vacuous pass. Fail instead."""
    columns = (
        _column(gate, "agents", "id", nullable=False, default="gen_random_uuid()"),
        _column(gate, "agents", "name", nullable=False),
    )

    with pytest.raises(gate.GateError) as excinfo:
        gate._plan_seed_statements(
            columns, approval_route_era=gate.APPROVAL_ROUTE_ERA_LEGACY
        )

    message = str(excinfo.value)
    assert "agents.slack_channel" in message
    assert "agent_channels" in message


def test_an_unfilled_mandatory_column_raises_naming_that_column(
    gate: ModuleType,
) -> None:
    """The next person to add a NOT NULL column is told what to add.

    Without this they get a bare Postgres not-null error from inside a CI gate
    they did not write.
    """

    columns = (
        *_legacy_columns(gate),
        _column(gate, "agents", "gate_probe_required", nullable=False),
    )

    with pytest.raises(gate.GateError) as excinfo:
        gate._plan_seed_statements(
            columns, approval_route_era=gate.APPROVAL_ROUTE_ERA_LEGACY
        )

    assert "gate_probe_required" in str(excinfo.value)


# --------------------------------------------------------------------------
# T5 -- the read-back runner's pure assertion helper
# --------------------------------------------------------------------------

PYDANTIC_TEXT = (
    "1 validation error for AgentOut\nchannel.address\n  Value error, "
    "slack channel 'C-0a1b2c3d' is not a Slack channel ID"
)


def _expected_names(gate: ModuleType) -> tuple[str, ...]:
    return tuple(agent.name for agent in gate.SEED_FIXTURE)


def _expected_addresses(gate: ModuleType) -> tuple[str, ...]:
    return tuple(agent.address for agent in gate.SEED_FIXTURE)


def _dump(readback: ModuleType, name: str, address: str) -> Any:
    """One agent that serialized cleanly, with its address nested as it really is."""
    return readback.AgentDump(
        name=name,
        addresses=(address,),
        dump={
            "name": name,
            "channels": [{"kind": "slack", "address": address}],
        },
        error=None,
    )


def _all_dumps(gate: ModuleType, readback: ModuleType) -> tuple[Any, ...]:
    return tuple(
        _dump(readback, agent.name, agent.address) for agent in gate.SEED_FIXTURE
    )


def test_an_empty_result_set_is_a_failure_not_a_pass(
    gate: ModuleType, readback: ModuleType
) -> None:
    """The vacuous-pass guard: zero rows means the seed did nothing."""
    failures = readback._collect_failures(
        (),
        expected_agents=_expected_names(gate),
        expected_addresses=_expected_addresses(gate),
    )

    assert failures
    assert any("no agents" in failure.lower() for failure in failures)


def test_all_expectations_met_reports_no_failures(
    gate: ModuleType, readback: ModuleType
) -> None:
    """Positive control for the helper."""
    failures = readback._collect_failures(
        _all_dumps(gate, readback),
        expected_agents=_expected_names(gate),
        expected_addresses=_expected_addresses(gate),
    )

    assert failures == ()


def test_a_dump_missing_an_expected_address_names_that_address(
    gate: ModuleType, readback: ModuleType
) -> None:
    """A migration that silently drops the seeded row must not read green."""
    dropped = gate.SEED_FIXTURE[1]
    dumps = tuple(
        readback.AgentDump(
            name=dropped.name, addresses=(), dump={"name": dropped.name}, error=None
        )
        if agent.name == dropped.name
        else _dump(readback, agent.name, agent.address)
        for agent in gate.SEED_FIXTURE
    )

    failures = readback._collect_failures(
        dumps,
        expected_agents=_expected_names(gate),
        expected_addresses=_expected_addresses(gate),
    )

    assert any(dropped.address in failure for failure in failures)


def test_a_missing_expected_agent_name_is_reported(
    gate: ModuleType, readback: ModuleType
) -> None:
    missing = gate.SEED_FIXTURE[0]
    dumps = tuple(
        _dump(readback, agent.name, agent.address)
        for agent in gate.SEED_FIXTURE
        if agent.name != missing.name
    )

    failures = readback._collect_failures(
        dumps,
        expected_agents=_expected_names(gate),
        expected_addresses=_expected_addresses(gate),
    )

    assert any(missing.name in failure for failure in failures)


def test_a_validation_failure_names_the_data_before_pydantic(
    gate: ModuleType, readback: ModuleType
) -> None:
    """#1914's literal complaint: "an error naming Pydantic rather than the data".

    Ordering, not membership. An operator reading the CI log must see WHICH agent
    and WHICH address before the traceback text explains itself.
    """

    broken = gate.SEED_FIXTURE[1]
    dumps = tuple(
        readback.AgentDump(
            name=broken.name,
            addresses=(broken.address,),
            dump=None,
            error=PYDANTIC_TEXT,
        )
        if agent.name == broken.name
        else _dump(readback, agent.name, agent.address)
        for agent in gate.SEED_FIXTURE
    )

    failures = readback._collect_failures(
        dumps,
        expected_agents=_expected_names(gate),
        expected_addresses=_expected_addresses(gate),
    )

    rendered = next(
        failure
        for failure in failures
        if broken.name in failure and "validation error" in failure
    )
    name_index = rendered.index(broken.name)
    address_index = rendered.index(broken.address)
    pydantic_index = rendered.index("1 validation error for AgentOut")
    assert name_index < pydantic_index
    assert address_index < pydantic_index


def _released_state_expectation(gate: ModuleType, readback: ModuleType) -> Any:
    seeded = _metadata_with_released_state(gate).legacy_state
    assert seeded is not None
    return readback.StateSentinelExpectation(
        owner_name=seeded.owner_name,
        namespace=seeded.namespace,
        key=seeded.key,
        value=seeded.value,
    )


def _released_state_observation(
    readback: ModuleType,
    expectation: Any,
    **overrides: Any,
) -> Any:
    values = {
        "owner_name": expectation.owner_name,
        "binding_scope": None,
        "namespace": expectation.namespace,
        "key": expectation.key,
        "value": expectation.value,
        "owner_memory": True,
    }
    values.update(overrides)
    return readback.StateSentinelObservation(**values)


def _released_state_failures(
    gate: ModuleType,
    readback: ModuleType,
    observations: tuple[Any, ...],
) -> tuple[str, ...]:
    return readback._collect_failures(
        _all_dumps(gate, readback),
        expected_agents=_expected_names(gate),
        expected_addresses=_expected_addresses(gate),
        state_expectation=_released_state_expectation(gate, readback),
        state_observations=observations,
    )


def test_released_state_readback_accepts_one_shared_unchanged_visible_sentinel(
    gate: ModuleType,
    readback: ModuleType,
) -> None:
    expectation = _released_state_expectation(gate, readback)
    reserved_neighbour = _released_state_observation(
        readback,
        expectation,
        namespace="memory",
        key="runner-reserved",
        value={"reserved": True},
    )
    sentinel = _released_state_observation(readback, expectation)

    failures = _released_state_failures(
        gate,
        readback,
        (reserved_neighbour, sentinel),
    )

    assert failures == ()


def test_released_state_readback_rejects_a_missing_sentinel(
    gate: ModuleType,
    readback: ModuleType,
) -> None:
    failures = _released_state_failures(gate, readback, ())

    rendered = "\n".join(failures)
    assert "exactly one" in rendered
    assert "found 0" in rendered


def test_released_state_readback_does_not_mistake_reserved_state_for_the_sentinel(
    gate: ModuleType,
    readback: ModuleType,
) -> None:
    expectation = _released_state_expectation(gate, readback)
    reserved_only = _released_state_observation(
        readback,
        expectation,
        namespace="transcript",
    )

    failures = _released_state_failures(gate, readback, (reserved_only,))

    rendered = "\n".join(failures)
    assert expectation.namespace in rendered
    assert "found 0" in rendered


def test_released_state_readback_rejects_duplicate_shared_identity(
    gate: ModuleType,
    readback: ModuleType,
) -> None:
    expectation = _released_state_expectation(gate, readback)
    sentinel = _released_state_observation(readback, expectation)

    failures = _released_state_failures(gate, readback, (sentinel, sentinel))

    rendered = "\n".join(failures)
    assert "exactly one" in rendered
    assert "found 2" in rendered


def test_released_state_readback_rejects_a_scoped_sentinel(
    gate: ModuleType,
    readback: ModuleType,
) -> None:
    expectation = _released_state_expectation(gate, readback)
    scoped = _released_state_observation(
        readback,
        expectation,
        binding_scope="slack:C0EXAMPLE1",
    )

    failures = _released_state_failures(gate, readback, (scoped,))

    rendered = "\n".join(failures)
    assert "binding_scope" in rendered
    assert "NULL" in rendered


def test_released_state_readback_rejects_a_changed_json_value(
    gate: ModuleType,
    readback: ModuleType,
) -> None:
    expectation = _released_state_expectation(gate, readback)
    changed = _released_state_observation(
        readback,
        expectation,
        value={"changed": True},
    )

    failures = _released_state_failures(gate, readback, (changed,))

    assert any("value" in failure.lower() for failure in failures)


def test_released_state_readback_rejects_an_owner_that_remains_memory_false(
    gate: ModuleType,
    readback: ModuleType,
) -> None:
    expectation = _released_state_expectation(gate, readback)
    hidden = _released_state_observation(
        readback,
        expectation,
        owner_memory=False,
    )

    failures = _released_state_failures(gate, readback, (hidden,))

    rendered = "\n".join(failures).lower()
    assert "memory" in rendered
    assert "true" in rendered


@pytest.mark.parametrize(
    ("revisions", "expected"),
    [
        (("0060_hook_runs_deferred.py",), False),
        (("0061_agent_channels_route_identity.py",), True),
        (("0061_something_else.py",), False),
    ],
)
def test_route_identity_readback_is_pinned_to_the_exact_0061_file(
    gate: ModuleType, tmp_path: Path, revisions: tuple[str, ...], expected: bool
) -> None:
    tree = _fake_released_tree(tmp_path, revisions=revisions)

    assert gate._candidate_supports_route_identity(tree) is expected


def test_route_identity_readback_argument_is_enabled_only_by_0061(
    gate: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: list[list[str]] = []

    def _fake_run_in_tree(command: list[str], **_kwargs: Any) -> Any:
        captured.append(command)
        return gate.CommandResult(0, "ok")

    monkeypatch.setattr(gate, "_run_in_tree", _fake_run_in_tree)
    for name, revision in (
        ("before", "0060_hook_runs_deferred.py"),
        ("after", "0061_agent_channels_route_identity.py"),
    ):
        gate._run_readback(
            _fake_released_tree(tmp_path / name, revisions=(revision,)),
            database_url="postgresql+asyncpg://gate/test",
            phase=gate.READBACK_PHASE,
            ref="candidate",
            commit="0" * 40,
            seed_metadata=gate.SeedMetadata(legacy_state=None),
        )

    assert "--expect-slack-identity" not in captured[0]
    assert _argument_value(captured[1], "--expect-slack-identity") == "default"


def test_readback_names_every_slack_binding_that_is_not_the_expected_identity(
    gate: ModuleType, readback: ModuleType
) -> None:
    failures = readback._collect_failures(
        _all_dumps(gate, readback),
        expected_agents=_expected_names(gate),
        expected_addresses=_expected_addresses(gate),
        slack_identity="default",
        slack_identity_observations=(("gate-valid", "C0EXAMPLE1", None),),
    )

    assert any("C0EXAMPLE1" in f and "'default'" in f for f in failures), failures


def test_readback_names_a_serialized_slack_binding_that_is_not_the_expected_identity(
    gate: ModuleType, readback: ModuleType
) -> None:
    dumps = tuple(
        readback.AgentDump(
            name=agent.name,
            addresses=(agent.address,),
            dump={
                "name": agent.name,
                "channels": [{"kind": "slack", "address": agent.address, "adapter": None}],
            },
            error=None,
        )
        for agent in gate.SEED_FIXTURE
    )
    observed = tuple((agent.name, agent.address, "default") for agent in gate.SEED_FIXTURE)

    failures = readback._collect_failures(
        dumps,
        expected_agents=_expected_names(gate),
        expected_addresses=_expected_addresses(gate),
        slack_identity="default",
        slack_identity_observations=observed,
    )

    assert failures
    assert all("serialized" in failure for failure in failures), failures


def test_readback_refuses_an_identity_expectation_that_observed_nothing(
    gate: ModuleType, readback: ModuleType
) -> None:
    failures = readback._collect_failures(
        _all_dumps(gate, readback),
        expected_agents=_expected_names(gate),
        expected_addresses=_expected_addresses(gate),
        slack_identity="default",
        slack_identity_observations=(),
    )

    assert any("no Slack binding" in failure for failure in failures), failures


def test_readback_passes_when_every_slack_binding_names_the_expected_identity(
    gate: ModuleType, readback: ModuleType
) -> None:
    observed = tuple((agent.name, agent.address, "default") for agent in gate.SEED_FIXTURE)

    failures = readback._collect_failures(
        _all_dumps(gate, readback),
        expected_agents=_expected_names(gate),
        expected_addresses=_expected_addresses(gate),
        slack_identity="default",
        slack_identity_observations=observed,
    )

    assert failures == ()

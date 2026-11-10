"""@spec MC001: the optional memory census reports metadata and fails closed."""

import asyncio
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "memory-census" / "memory_census.py"


def _load_census():
    spec = importlib.util.spec_from_file_location("memory_census_example", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows, fail=False):
        self.rows = rows
        self.fail = fail
        self.statement = None
        self.parameters = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def execute(self, statement, *parameters):
        self.statement = str(statement).lower()
        self.parameters = parameters
        if self.fail:
            raise RuntimeError("private-db-url-with-secret")
        return _Result(self.rows)


class _Engine:
    def __init__(self, connection):
        self.connection = connection
        self.disposed = False

    def connect(self):
        return self.connection

    async def dispose(self):
        self.disposed = True


def _run_with_rows(monkeypatch, capsys, rows):
    module = _load_census()
    connection = _Connection(rows)
    engine = _Engine(connection)
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://example@localhost/example")
    monkeypatch.setattr(module, "create_async_engine", lambda _: engine)
    asyncio.run(module.main())
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert engine.disposed
    return output, connection


def test_memory_census_reports_only_metadata_and_counts(monkeypatch, capsys):
    """@spec MC001 c1-c3: selected rows produce deterministic metadata only."""
    output, connection = _run_with_rows(
        monkeypatch,
        capsys,
        [("agent-alpha", "first", 5), ("agent-alpha", "second", 11),
         ("agent-beta", "third", 19)],
    )
    assert output == [
        {"memory_census": "entry", "agent": "agent-alpha", "key": "first", "bytes": 5},
        {"memory_census": "entry", "agent": "agent-alpha", "key": "second", "bytes": 11},
        {"memory_census": "entry", "agent": "agent-beta", "key": "third", "bytes": 19},
        {"memory_census": "agent", "agent": "agent-alpha", "rows": 2},
        {"memory_census": "agent", "agent": "agent-beta", "rows": 1},
        {"memory_census": "total", "rows": 3, "agents": 2},
    ]
    statement = connection.statement
    assert statement is not None
    assert "curie.workflow_state_entries" in statement
    assert "curie.agents" in statement
    assert "namespace" in statement
    assert "memory" in statement or "memory" in str(connection.parameters)
    assert "octet_length" in statement
    assert "order by" in statement
    assert statement.lstrip().startswith("select")
    assert "insert" not in statement and "update" not in statement and "delete" not in statement


def test_memory_census_empty_store_has_real_zero_total(monkeypatch, capsys):
    """@spec MC001 c3: an empty successful read has one zero total."""
    output, _ = _run_with_rows(monkeypatch, capsys, [])
    assert output == [{"memory_census": "total", "rows": 0, "agents": 0}]


@pytest.mark.parametrize(
    "rows,fail", [([], True), ([("agent-alpha", "first", 5), ("broken",)], False)]
)
def test_memory_census_collection_failure_has_no_success_output(monkeypatch, capsys, rows, fail):
    """@spec MC001 c4: collection must finish before any success line escapes."""
    module = _load_census()
    engine = _Engine(_Connection(rows, fail=fail))
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://example@localhost/example")
    monkeypatch.setattr(module, "create_async_engine", lambda _: engine)
    try:
        asyncio.run(module.main())
    except Exception:  # noqa: BLE001 - test both query and result-decoding failures
        pass
    assert capsys.readouterr().out == ""
    assert engine.disposed


@pytest.mark.parametrize("database_url", [None, "not-a-database-url"])
def test_memory_census_cli_errors_are_typed_and_credential_free(database_url):
    """@spec MC001 c4: a failing CLI emits only a typed JSON error."""
    env = os.environ.copy()
    if database_url is None:
        env.pop("DATABASE_URL", None)
    else:
        env["DATABASE_URL"] = database_url
    done = subprocess.run(
        [sys.executable, str(SCRIPT)], capture_output=True, text=True, env=env, timeout=20
    )
    assert done.returncode == 1
    assert done.stdout == ""
    error_lines = done.stderr.splitlines()
    assert len(error_lines) == 1
    error = json.loads(error_lines[0])
    assert set(error) == {"error"}
    assert error["error"] in {"configuration_error", "collection_error"}
    assert "not-a-database-url" not in done.stderr

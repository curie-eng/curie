"""Actual standalone startup, @spec PROTECTED-HOOK-SOURCE-2/10."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from aci_protocol.service_config import HEARTBEAT_FILE_ENV, STREAM_ENV
from curie_test_support.valkey import connect_or_skip
from curie_worker import run
from curie_worker.config import WorkerConfig
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[3]


def migration_support() -> Any:
    """Test setup only, @spec PROTECTED-HOOK-SOURCE-2/10."""
    path = ROOT / "apps/api/tests/_migration_support.py"
    spec = importlib.util.spec_from_file_location("_source_worker_migration_support", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def worker_templates() -> Iterator[Any]:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    support = migration_support()
    base = make_url(support.get_settings().database_url)
    templates = support.MigrationTemplates(base)
    try:
        yield support, base, templates
    finally:
        templates.drop_all()


@pytest.fixture
def worker_db(worker_templates: Any) -> Iterator[Any]:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    support, base, templates = worker_templates
    name = "source_worker_" + uuid.uuid4().hex
    saved = os.environ.get("DATABASE_URL")
    clone = support.IsolatedMigrationDb(base, name, templates)
    try:
        clone.at("0090")
        url = support.render_url(base.set(database=name))
        os.environ["DATABASE_URL"] = url
        support.get_settings.cache_clear()
        yield support, clone, url
    finally:
        if saved is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = saved
        support.get_settings.cache_clear()
        asyncio.run(support.admin_execute(base, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))


def adapter() -> Any:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    method = getattr(run, "assert_worker_schema", None)
    assert callable(method), "SOURCE-2 standalone worker schema startup adapter is missing"
    return method


@pytest.mark.parametrize("revision", ["0090", "future-expand"])
def test_worker_adapter_reads_actual_compatible_schema_without_writes_or_live_backend(
    worker_db: Any, revision: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    support, _, url = worker_db
    method = adapter()
    support.sql_dicts("UPDATE curie.alembic_version SET version_num=:r", {"r": revision})
    before = support.sql_dicts("SELECT * FROM curie.alembic_version")
    config = WorkerConfig(database_url=url)
    asyncio.run(asyncio.wait_for(method(config), 35))
    assert support.sql_dicts("SELECT * FROM curie.alembic_version") == before
    assert support.sql_dicts(
        "SELECT count(*) AS peers FROM pg_stat_activity "
        "WHERE datname=current_database() AND pid<>pg_backend_pid()"
    ) == [{"peers": 0}]


@pytest.mark.parametrize("kind", ["old", "missing", "future_missing"])
def test_worker_adapter_refuses_actual_incompatible_schema_without_booting(
    worker_db: Any, kind: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    support, clone, url = worker_db
    method = adapter()
    if kind == "old":
        clone.at("0075")
    else:
        if kind == "future_missing":
            support.sql_dicts("UPDATE curie.alembic_version SET version_num='future-expand'")
        support.sql_dicts("DROP TABLE curie.hook_source_operations")
    before = support.sql_dicts("SELECT * FROM curie.alembic_version")
    with pytest.raises(RuntimeError) as caught:
        asyncio.run(asyncio.wait_for(method(WorkerConfig(database_url=url)), 35))
    assert getattr(caught.value, "code", None) == (
        "schema_below_min" if kind == "old" else "schema_structure_unavailable"
    )
    assert url not in str(caught.value)
    assert support.sql_dicts("SELECT * FROM curie.alembic_version") == before
    assert support.sql_dicts(
        "SELECT count(*) AS peers FROM pg_stat_activity "
        "WHERE datname=current_database() AND pid<>pg_backend_pid()"
    ) == [{"peers": 0}]


def test_worker_adapter_actual_dependency_failure_has_safe_category(worker_db: Any) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2."""
    _, _, url = worker_db
    method = adapter()
    name = "missing_worker_" + uuid.uuid4().hex
    unavailable = make_url(url).set(database=name).render_as_string(hide_password=False)
    with pytest.raises(RuntimeError) as caught:
        asyncio.run(asyncio.wait_for(method(WorkerConfig(database_url=unavailable)), 35))
    assert getattr(caught.value, "code", None) == "schema_probe_unavailable"
    assert name not in str(caught.value) and "postgresql" not in str(caught.value)


@pytest.mark.parametrize("kind", ["old", "future_missing"])
def test_actual_worker_process_refuses_schema_before_invalid_build_or_boot_effects(
    worker_db: Any, tmp_path: Path, kind: str
) -> None:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    support, clone, url = worker_db
    if kind == "old":
        clone.at("0075")
    else:
        support.sql_dicts("UPDATE curie.alembic_version SET version_num='future-expand'")
        support.sql_dicts("DROP TABLE curie.hook_source_operations")
    heartbeat = tmp_path / "worker-heartbeat"
    token = uuid.uuid4().hex
    stream, eval_stream = "test:source-worker:runs:" + token, "test:source-worker:evals:" + token
    client = connect_or_skip()
    marker = "test:source-worker:preboot:" + token
    client.set(marker, "unchanged")
    before = support.sql_dicts("SELECT * FROM curie.alembic_version")
    program = '''
import asyncio,json,os
from curie_worker.run import _run
from curie_worker.config import WorkerConfig
async def exercise():
    """@spec PROTECTED-HOOK-SOURCE-2."""
    try: await _run(WorkerConfig(),os.environ)
    except BaseException as error:
        print(json.dumps({'code':getattr(error,'code',None),'type':type(error).__name__}))
    else: print(json.dumps({'code':'unexpected_success'}))
asyncio.run(exercise())
'''
    environment = dict(
        os.environ,
        DATABASE_URL=url,
        CURIE_SANDBOX_SUBSTRATE="kubernetes",
        CURIE_CLAIM_TIMEOUT_SECONDS="0",
        CURIE_EVAL_STREAM=eval_stream,
        OTEL_SDK_DISABLED="true",
        CURIE_INTERNAL_WORKER_TOKEN="",
        CURIE_WORKSPACE_ENABLED="false",
    )
    environment[STREAM_ENV] = stream
    environment[HEARTBEAT_FILE_ENV] = str(heartbeat)
    try:
        result = subprocess.run(
            [sys.executable, "-c", program],
            env=environment,
            capture_output=True,
            text=True,
            timeout=40,
        )
        assert result.returncode == 0, "owned worker startup process failed unexpectedly"
        outcome = json.loads(result.stdout)
        assert outcome["code"] == (
            "schema_below_min" if kind == "old" else "schema_structure_unavailable"
        )
        assert (
            not heartbeat.exists() and not client.exists(stream) and not client.exists(eval_stream)
        )
        assert client.get(marker) == "unchanged"
        assert support.sql_dicts("SELECT * FROM curie.alembic_version") == before
    finally:
        client.delete(marker, stream, eval_stream)
        client.close()

"""Naming the connector Deployment to read, against a real agents row. @spec ACTION-EXECUTOR-12.

The digest read is a single-object ``get``, so the name is computed, not
searched for. It must be the name the reconciler rendered:
``plugin_format.connector_render.object_name(release, agent name, connector)``,
with the agent's name read from the same ``agents`` table the connector
reconcile loop reads. A name that drifts from the render reads nothing (null
digest forever) or, worse, another agent's Deployment.
"""

from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path
from typing import Any

import pytest
from curie_worker.action_digest import agent_deployment_resolver
from plugin_format.connector_render import object_name
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.anyio


def _startup_support() -> Any:
    path = Path(__file__).with_name("test_source_worker_startup.py")
    spec = importlib.util.spec_from_file_location("_action_digest_resolver_setup", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_startup = _startup_support()
worker_db = _startup.worker_db
worker_templates = _startup.worker_templates

RELEASE = "rel"


@pytest.fixture
def agent(worker_db: Any) -> tuple[str, uuid.UUID, str]:
    support, _, url = worker_db
    agent_id = uuid.uuid4()
    name = "ops-" + agent_id.hex[:8]
    support.sql_dicts(
        "INSERT INTO curie.agents(id,name) VALUES(:id,:name)", {"id": agent_id, "name": name}
    )
    return url, agent_id, name


async def test_the_resolved_name_is_the_rendered_object_name(
    agent: tuple[str, uuid.UUID, str],
) -> None:
    url, agent_id, name = agent
    engine = create_async_engine(url)
    try:
        resolve = agent_deployment_resolver(engine, db_schema="curie", release=RELEASE)
        resolved = await resolve(str(agent_id), "grafana")
    finally:
        await engine.dispose()

    assert resolved == object_name(RELEASE, name, "grafana")
    assert resolved == f"{RELEASE}-{name}-mcp-grafana"


async def test_an_unknown_or_absent_agent_names_nothing(
    agent: tuple[str, uuid.UUID, str],
) -> None:
    url, _, _ = agent
    engine = create_async_engine(url)
    try:
        resolve = agent_deployment_resolver(engine, db_schema="curie", release=RELEASE)
        unknown = await resolve(str(uuid.uuid4()), "grafana")
        absent = await resolve(None, "grafana")
    finally:
        await engine.dispose()

    assert unknown is None
    assert absent is None

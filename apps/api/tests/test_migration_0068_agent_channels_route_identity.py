"""Migration 0061: a Slack route names its identity, and the key is the triple.

ADR-0168 decision 3. A throwaway database per test (`isolated_migration_db`),
real Postgres, raw SQL seeds: the constraints are database invariants because
an out-of-band writer (a restored dump, an operator's psql) has no validator.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from curie_api.models import AgentChannel
from sqlalchemy import CheckConstraint, UniqueConstraint
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import text

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"

# Named, never relative: a later head would make "-1" stop short (#1391).
BELOW = "0060"
REVISION = "0061"
ROUTE_KEY = "agent_channels_route_key"
ROUTE_CK = "agent_channels_route_ck"
SECRET_ENDPOINT = "http://127.0.0.1:1/replies?token=never-echo-me"


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[Any]:
    async def go() -> list[Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params or {})
                return list(result.all()) if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _cfg() -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(ALEMBIC_DIR))
    return cfg


def _agent(name: str, routes: dict[str, Any] | None = None) -> uuid.UUID:
    agent_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name, approval_routes) "
        "VALUES (:id, :name, CAST(:routes AS jsonb))",
        {"id": agent_id, "name": name, "routes": json.dumps(routes) if routes else None},
    )
    return agent_id


def _bind(
    agent: uuid.UUID,
    kind: str,
    address: str,
    *,
    endpoint: str | None = None,
    adapter: str | None = None,
) -> None:
    _sql(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address, endpoint, adapter) "
        "VALUES (:id, :agent, :kind, :address, :endpoint, :adapter)",
        {
            "id": uuid.uuid4(),
            "agent": agent,
            "kind": kind,
            "address": address,
            "endpoint": endpoint,
            "adapter": adapter,
        },
    )


def _approval(kind: str, channel: str, *, endpoint: str | None = None) -> uuid.UUID:
    approval_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.approvals (id, conversation_id, author, summary, reply_kind, "
        "reply_channel, reply_placeholder, reply_endpoint, dedupe_key, status) "
        "VALUES (:id, 'th-0061', 'U1', 'seeded', :kind, :channel, NULL, :endpoint, "
        ":dedupe, 'pending')",
        {
            "id": approval_id,
            "kind": kind,
            "channel": channel,
            "endpoint": endpoint,
            "dedupe": uuid.uuid4().hex,
        },
    )
    return approval_id


def _publication(agent: uuid.UUID, approval: uuid.UUID, channel: str) -> uuid.UUID:
    """A terminal publication: `failed` needs no lineage (`publications_active_lineage_ck`)."""

    version_id, deployment_id, publication_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    _sql(
        "INSERT INTO curie.agent_versions (id, agent_id, version_label, bundle_ref, created_by) "
        "VALUES (:id, :agent, 'v1', 'bundles/v1.tar.gz', 'migration-test')",
        {"id": version_id, "agent": agent},
    )
    _sql(
        "INSERT INTO curie.deployments (id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent, :version, CAST('prod' AS curie.environment), 'active')",
        {"id": deployment_id, "agent": agent, "version": version_id},
    )
    _sql(
        "INSERT INTO curie.publications (id, approval_id, deployment_id, repo_full_name, "
        "status, base_sha, changed_paths, title, body, reply_kind, reply_channel) "
        "VALUES (:id, :approval, :deployment, 'acme/app', 'failed', :sha, "
        "CAST('[]' AS jsonb), 't', 'b', 'slack', :channel)",
        {
            "id": publication_id,
            "approval": approval,
            "deployment": deployment_id,
            "sha": "0" * 40,
            "channel": channel,
        },
    )
    return publication_id


@pytest.mark.usefixtures("isolated_migration_db")
def test_upgrade_names_the_default_identity_on_every_slack_route() -> None:
    command.upgrade(_cfg(), BELOW)
    agent = _agent("a")
    _bind(agent, "slack", "C0EXAMPLE1")
    _bind(agent, "email", "ops@example.com")
    stub = _approval("slack", "C0EXAMPLE1", endpoint="http://cli-stub.test/api/")
    mail = _approval("email", "ops@example.com")
    publication = _publication(agent, stub, "C0EXAMPLE1")

    command.upgrade(_cfg(), REVISION)

    rows = dict(_sql("SELECT kind, adapter FROM curie.agent_channels"))
    assert rows == {"slack": "default", "email": None}
    replies = dict(_sql("SELECT id, reply_adapter FROM curie.approvals"))
    # A Slack reply endpoint is the CLI stub's per-turn endpoint, not a transport.
    assert replies == {stub: "default", mail: None}
    assert _sql(
        "SELECT reply_adapter FROM curie.publications WHERE id = :id", {"id": publication}
    ) == [("default",)]


@pytest.mark.usefixtures("isolated_migration_db")
def test_upgrade_refuses_a_slack_binding_that_carries_an_endpoint_by_name() -> None:
    command.upgrade(_cfg(), BELOW)
    agent = _agent("custom-transport")
    _bind(agent, "slack", "C0EXAMPLE2", endpoint=SECRET_ENDPOINT, adapter="proof-offline")

    with pytest.raises(RuntimeError) as err:
        command.upgrade(_cfg(), REVISION)

    message = str(err.value)
    assert message.startswith("cannot upgrade to 0061:"), message
    assert "custom-transport" in message and "C0EXAMPLE2" in message
    assert "proof-offline" in message
    assert "never-echo-me" not in message and "127.0.0.1" not in message
    assert _sql("SELECT version_num FROM curie.alembic_version") == [(BELOW,)]


@pytest.mark.usefixtures("isolated_migration_db")
def test_upgrade_refuses_a_slack_notification_target_that_carries_an_endpoint() -> None:
    command.upgrade(_cfg(), BELOW)
    routes = {
        "deploy": {
            "resolution": {"kind": "slack", "address": "C0EXAMPLE3"},
            "notification": {
                "kind": "slack",
                "address": "C0EXAMPLE4",
                "endpoint": SECRET_ENDPOINT,
                "adapter": "proof-offline",
            },
        }
    }
    agent = _agent("notifier", routes)
    _bind(agent, "slack", "C0EXAMPLE3")

    with pytest.raises(RuntimeError) as err:
        command.upgrade(_cfg(), REVISION)

    message = str(err.value)
    assert message.startswith("cannot upgrade to 0061:"), message
    assert "notifier" in message and "'deploy'" in message, message
    assert "C0EXAMPLE4" in message
    assert "never-echo-me" not in message and "127.0.0.1" not in message
    assert _sql("SELECT version_num FROM curie.alembic_version") == [(BELOW,)]


@pytest.mark.usefixtures("isolated_migration_db")
def test_a_slack_route_without_an_identity_is_refused_by_the_route_check() -> None:
    command.upgrade(_cfg(), REVISION)
    with pytest.raises(IntegrityError) as err:
        _bind(_agent("a"), "slack", "C0EXAMPLE1")
    assert ROUTE_CK in str(err.value)


@pytest.mark.usefixtures("isolated_migration_db")
def test_a_slack_route_with_an_endpoint_is_refused_by_the_route_check() -> None:
    command.upgrade(_cfg(), REVISION)
    with pytest.raises(IntegrityError) as err:
        _bind(_agent("a"), "slack", "C0EXAMPLE1", endpoint="http://x.example", adapter="default")
    assert ROUTE_CK in str(err.value)


@pytest.mark.usefixtures("isolated_migration_db")
def test_a_non_slack_route_stays_both_or_neither() -> None:
    command.upgrade(_cfg(), REVISION)
    agent = _agent("a")
    with pytest.raises(IntegrityError) as err:
        _bind(agent, "email", "a@example.com", adapter="agentmail")
    assert ROUTE_CK in str(err.value)
    _bind(agent, "email", "b@example.com", endpoint="https://m.example", adapter="agentmail")
    _bind(agent, "email", "c@example.com")
    assert _sql(
        "SELECT adapter FROM curie.agent_channels WHERE address = 'c@example.com'"
    ) == [(None,)]


@pytest.mark.usefixtures("isolated_migration_db")
def test_two_identities_share_a_slack_channel_but_one_identity_binds_it_once() -> None:
    command.upgrade(_cfg(), REVISION)
    a, b = _agent("a"), _agent("b")
    _bind(a, "slack", "C0EXAMPLE1", adapter="default")
    _bind(b, "slack", "C0EXAMPLE1", adapter="second")
    _bind(a, "slack", "C0EXAMPLE1", adapter="third")
    with pytest.raises(IntegrityError) as err:
        _bind(b, "slack", "C0EXAMPLE1", adapter="default")
    assert ROUTE_KEY in str(err.value)


@pytest.mark.usefixtures("isolated_migration_db")
def test_route_less_non_slack_bindings_still_collide_on_the_pair() -> None:
    command.upgrade(_cfg(), REVISION)
    _bind(_agent("a"), "email", "ops@example.com")
    with pytest.raises(IntegrityError) as err:
        _bind(_agent("b"), "email", "ops@example.com")
    assert ROUTE_KEY in str(err.value)


@pytest.mark.usefixtures("isolated_migration_db")
def test_the_route_key_leads_with_the_pair_so_pair_lookups_keep_its_index() -> None:
    command.upgrade(_cfg(), REVISION)
    definition = _sql(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conrelid = 'curie.agent_channels'::regclass AND conname = :name",
        {"name": ROUTE_KEY},
    )
    assert definition == [("UNIQUE NULLS NOT DISTINCT (kind, address, adapter)",)]


@pytest.mark.usefixtures("isolated_migration_db")
def test_downgrade_refuses_a_named_slack_identity_by_name() -> None:
    command.upgrade(_cfg(), REVISION)
    _bind(_agent("named"), "slack", "C0EXAMPLE3", adapter="second")
    with pytest.raises(RuntimeError) as err:
        command.downgrade(_cfg(), BELOW)
    message = str(err.value)
    assert message.startswith("cannot downgrade below 0061:"), message
    assert "'second'" in message and "C0EXAMPLE3" in message and "named" in message
    assert _sql("SELECT version_num FROM curie.alembic_version") == [(REVISION,)]


@pytest.mark.usefixtures("isolated_migration_db")
def test_downgrade_refuses_two_routes_that_collapse_onto_one_pair() -> None:
    command.upgrade(_cfg(), REVISION)
    a, b = _agent("a"), _agent("b")
    _bind(a, "email", "ops@example.com", endpoint="https://a.example", adapter="inbox-a")
    _bind(b, "email", "ops@example.com", endpoint="https://b.example", adapter="inbox-b")
    with pytest.raises(RuntimeError) as err:
        command.downgrade(_cfg(), BELOW)
    message = str(err.value)
    assert message.startswith("cannot downgrade below 0061:"), message
    assert str(a) in message and str(b) in message and "ops@example.com" in message
    assert "a.example" not in message and "b.example" not in message


@pytest.mark.usefixtures("isolated_migration_db")
def test_downgrade_restores_the_pair_and_never_touches_a_generation() -> None:
    command.upgrade(_cfg(), REVISION)
    agent = _agent("a")
    _bind(agent, "slack", "C0EXAMPLE4", adapter="default")
    approval = _approval("slack", "C0EXAMPLE4")
    _sql("UPDATE curie.approvals SET reply_adapter = 'default' WHERE id = :id", {"id": approval})
    _sql("UPDATE curie.agent_channels SET generation = 7")

    command.downgrade(_cfg(), BELOW)

    assert _sql("SELECT adapter, generation FROM curie.agent_channels") == [(None, 7)]
    assert _sql("SELECT reply_adapter FROM curie.approvals") == [(None,)]
    constraints = dict(
        _sql(
            "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'curie.agent_channels'::regclass"
        )
    )
    assert constraints["agent_channels_kind_address_key"] == "UNIQUE (kind, address)"
    assert "agent_channels_route_pair_ck" in constraints
    assert ROUTE_KEY not in constraints and ROUTE_CK not in constraints


def test_the_orm_declares_the_route_key_and_check() -> None:
    table = AgentChannel.__table__
    key = next(
        c for c in table.constraints if isinstance(c, UniqueConstraint) and c.name == ROUTE_KEY
    )
    assert [col.name for col in key.columns] == ["kind", "address", "adapter"]
    assert key.dialect_options["postgresql"]["nulls_not_distinct"] is True
    assert any(isinstance(c, CheckConstraint) and c.name == ROUTE_CK for c in table.constraints)

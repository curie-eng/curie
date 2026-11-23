"""Migration 0034 splits approval notification from the resolution target.

The runtime intentionally has no compatibility reader for the retired
``{"channel": ...}`` binding. The data revision therefore owns the cutover for
existing durable agent rows, atomically and without changing route names or
approver policy. Downgrade is loss-aware: a second notification target or a
non-Slack resolution cannot be represented by the old shape and must be refused
before any row is rewritten.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_rows
from alembic import command

BELOW = "0033"


def _seed_agent(name: str, routes: dict[str, Any] | None) -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name, approval_routes) "
        "VALUES (:id, :name, CAST(:routes AS jsonb))",
        {
            "id": agent_id,
            "name": name,
            "routes": None if routes is None else json.dumps(routes),
        },
    )
    return agent_id


def _routes_by_name() -> dict[str, dict[str, Any] | None]:
    rows = sql_rows("SELECT name, approval_routes FROM curie.agents ORDER BY name")
    return {row[0]: row[1] for row in rows}


def test_upgrade_rewrites_legacy_routes_preserving_approvers(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    cfg = alembic_config()
    isolated_migration_db.at(BELOW)
    _seed_agent(
        "legacy-route",
        {
            "managers": {
                "channel": "C0EXAMPLE1",
                "approvers": {"group": "S0EXAMPLE1"},
            },
            "legal": {"channel": "C0EXAMPLE2"},
        },
    )
    _seed_agent("empty-routes", {})
    _seed_agent("null-routes", None)
    json_null_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name, approval_routes) "
        "VALUES (:id, :name, 'null'::jsonb)",
        {"id": json_null_id, "name": "json-null-routes"},
    )

    command.upgrade(cfg, "head")

    json_null_preserved = sql_rows(
        "SELECT approval_routes = 'null'::jsonb FROM curie.agents WHERE id = :id",
        {"id": json_null_id},
    )
    assert json_null_preserved == [(True,)]

    assert _routes_by_name() == {
        "empty-routes": {},
        "json-null-routes": None,
        "legacy-route": {
            "managers": {
                "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
                "approvers": {"group": "S0EXAMPLE1"},
            },
            "legal": {"resolution": {"kind": "slack", "address": "C0EXAMPLE2"}},
        },
        "null-routes": None,
    }


def test_upgrade_refuses_malformed_legacy_routes_without_partial_rewrite(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    cfg = alembic_config()
    isolated_migration_db.at(BELOW)
    _seed_agent("a-valid", {"managers": {"channel": "C0EXAMPLE1"}})
    _seed_agent("z-malformed", {"legal": {"channel": 17}})
    before = _routes_by_name()

    with pytest.raises(RuntimeError) as caught:
        command.upgrade(cfg, "head")

    message = str(caught.value)
    assert "z-malformed" in message
    assert "legal" in message
    assert _routes_by_name() == before


def test_downgrade_restores_lossless_slack_only_routes(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    cfg = alembic_config()
    isolated_migration_db.at(BELOW)
    _seed_agent(
        "round-trip",
        {
            "managers": {
                "channel": "C0EXAMPLE1",
                "approvers": {"users": ["U0EXAMPLE1"]},
            },
            "legal": {"channel": "C0EXAMPLE2"},
        },
    )

    command.upgrade(cfg, "head")
    command.downgrade(cfg, BELOW)

    assert _routes_by_name() == {
        "round-trip": {
            "managers": {
                "channel": "C0EXAMPLE1",
                "approvers": {"users": ["U0EXAMPLE1"]},
            },
            "legal": {"channel": "C0EXAMPLE2"},
        }
    }


@pytest.mark.parametrize(
    ("agent_name", "routes", "reason"),
    [
        (
            "notified-agent",
            {
                "managers": {
                    "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
                    "notification": {
                        "kind": "email",
                        "address": "approvals@example.com",
                        "endpoint": "https://adapter.example.com/replies",
                        "adapter": "mail",
                    },
                }
            },
            "notification target",
        ),
        (
            "future-resolver",
            {
                "managers": {
                    "resolution": {"kind": "email", "address": "human@example.com"}
                }
            },
            "email",
        ),
    ],
    ids=("notification-target", "non-slack-resolution"),
)
def test_downgrade_refuses_lossy_split_routes_without_partial_rewrite(
    agent_name: str,
    routes: dict[str, Any],
    reason: str,
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    cfg = alembic_config()
    isolated_migration_db.at(BELOW)
    agent_id = _seed_agent(agent_name, {"managers": {"channel": "C0EXAMPLE1"}})
    _seed_agent("healthy-agent", {"legal": {"channel": "C0EXAMPLE2"}})
    command.upgrade(cfg, "head")
    sql_rows(
        "UPDATE curie.agents SET approval_routes = CAST(:routes AS jsonb) WHERE id = :id",
        {
            "id": agent_id,
            "routes": json.dumps(routes),
        },
    )
    before = _routes_by_name()

    with pytest.raises(RuntimeError) as caught:
        command.downgrade(cfg, BELOW)

    message = str(caught.value)
    assert agent_name in message
    assert "managers" in message
    assert reason in message
    assert _routes_by_name() == before

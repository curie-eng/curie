"""Migration 0148 binds executions to tenant principals and enrollments (#5376 AC6).

An expand revision above 0146. It adds ``curie.personal_claude_enrollments``
and the owner tuple (``tenant_id``, ``model_auth_mode``, ``owner_principal_id``,
``enrollment_generation``) on ``work_items`` and ``execution_requests``.
Existing rows upgrade as service rows with ``tenant_id`` backfilled from their
agent; composite foreign keys and ``IS TRUE`` tuple checks refuse every
incomplete or cross-tenant tuple; and the downgrade refuses while any personal
WorkItem or enrollment remains. Every refusal probe is paired with the insert or
update that the same schema accepts, so a probe cannot pass by tripping an
unrelated constraint.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from sqlalchemy.exc import IntegrityError

REVISION = "0148"
BELOW = "0146"
DEFAULT_TENANT = UUID("00000000-0000-0000-0000-000000000001")
OWNER_COLUMNS = {"tenant_id", "model_auth_mode", "owner_principal_id", "enrollment_generation"}
WAIT_DEADLINE = datetime(2026, 11, 5, 13, 0, tzinfo=UTC)
TERMINAL_AT = datetime(2026, 11, 5, 12, 30, tzinfo=UTC)


def _insert(table: str, values: dict[str, Any]) -> None:
    columns = ", ".join(values)
    params = ", ".join(f":{name}" for name in values)
    sql_rows(f"INSERT INTO curie.{table} ({columns}) VALUES ({params})", values)


def _refused(table: str, values: dict[str, Any]) -> None:
    with pytest.raises(IntegrityError):
        _insert(table, values)


def _update_refused(statement: str, params: dict[str, Any]) -> None:
    with pytest.raises(IntegrityError):
        sql_rows(statement, params)


def _tables() -> set[str]:
    return {
        row["table_name"]
        for row in sql_dicts(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'curie'"
        )
    }


def _tenant() -> UUID:
    tenant_id = uuid.uuid4()
    _insert(
        "tenants",
        {"id": tenant_id, "deployment_id": f"acme-migration-{tenant_id}", "status": "active"},
    )
    return tenant_id


def _principal(tenant_id: UUID) -> UUID:
    principal_id = uuid.uuid4()
    _insert(
        "principals",
        {
            "id": principal_id,
            "tenant_id": tenant_id,
            "idp_subject": f"acme-migration-{principal_id}",
            "type": "human",
            "status": "active",
        },
    )
    return principal_id


def _agent(tenant_id: UUID) -> UUID:
    agent_id = uuid.uuid4()
    _insert(
        "agents",
        {"id": agent_id, "tenant_id": tenant_id, "name": f"acme-bot-{agent_id.hex[:8]}"},
    )
    return agent_id


def _work_item(agent_id: UUID, issue: str, **owner: Any) -> dict[str, Any]:
    """A WorkItem row; ``owner`` names exactly the owner tuple columns to write."""

    return {
        "id": uuid.uuid4(),
        "tracker_kind": "github",
        "tracker_host": "github.com",
        "tracker_scope_id": "101",
        "tracker_issue_id": issue,
        "code_host_kind": "github",
        "code_host_host": "github.com",
        "repository_project_id": "101",
        "code_host_installation_id": 202,
        "agent_id": agent_id,
        "repository_path": "acme-corp/acme-bot",
        "conversation_id": f"acme-migration-{issue}",
        "version": 1,
        "next_sequence": 1,
        **owner,
    }


def _queued(work_item_id: UUID, sequence: int, **owner: Any) -> dict[str, Any]:
    """A queued request row; ``owner`` names exactly the owner tuple columns to write."""

    return {
        "id": uuid.uuid4(),
        "work_item_id": work_item_id,
        "sequence": sequence,
        "status": "queued",
        "version": 1,
        "execution_attempts": 0,
        **owner,
    }


def _personal(tenant_id: UUID, principal_id: UUID | None, generation: int | None) -> dict[str, Any]:
    return {
        "tenant_id": tenant_id,
        "model_auth_mode": "personal",
        "owner_principal_id": principal_id,
        "enrollment_generation": generation,
    }


def _enrollment(
    tenant_id: UUID, principal_id: UUID, *, generation: int = 1, version: int = 1
) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "tenant_id": tenant_id,
        "principal_id": principal_id,
        "generation": generation,
        "active": True,
        "version": version,
    }


def _row(table: str, row_id: UUID) -> dict[str, Any]:
    [row] = sql_dicts(f"SELECT * FROM curie.{table} WHERE id = :id", {"id": row_id})
    return row


def _seed_service_rows() -> dict[str, Any]:
    """At 0146: a non-default tenant's agent and a default-tenant agent, each
    with a WorkItem holding one terminal and one waiting request."""

    tenant = _tenant()
    seeded: dict[str, Any] = {"tenant": tenant, "items": {}, "requests": {}}
    for owner_tenant, issue in ((tenant, "7001"), (DEFAULT_TENANT, "7002")):
        agent = _agent(owner_tenant)
        item = _work_item(agent, issue)
        _insert("work_items", item)
        expired = {
            "id": uuid.uuid4(),
            "work_item_id": item["id"],
            "sequence": 1,
            "status": "expired",
            "version": 2,
            "execution_attempts": 0,
            "wait_deadline": WAIT_DEADLINE,
            "terminal_at": TERMINAL_AT,
            "terminal_cause": "capacity_wait_expired",
        }
        waiting = {
            "id": uuid.uuid4(),
            "work_item_id": item["id"],
            "sequence": 2,
            "status": "waiting",
            "version": 1,
            "execution_attempts": 0,
            "wait_deadline": WAIT_DEADLINE,
        }
        _insert("execution_requests", expired)
        _insert("execution_requests", waiting)
        seeded["items"][item["id"]] = owner_tenant
        seeded["requests"][expired["id"]] = owner_tenant
        seeded["requests"][waiting["id"]] = owner_tenant
    return seeded


def test_0148_upgrade_backfills_service_rows_enforces_the_tuple_and_guards_downgrade(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    assert "personal_claude_enrollments" not in _tables()
    assert not OWNER_COLUMNS & column_names("work_items")
    assert not OWNER_COLUMNS & column_names("execution_requests")
    seeded = _seed_service_rows()
    before = {
        **{row_id: _row("work_items", row_id) for row_id in seeded["items"]},
        **{row_id: _row("execution_requests", row_id) for row_id in seeded["requests"]},
    }

    command.upgrade(config, REVISION)

    assert stamped_revision() == REVISION
    assert OWNER_COLUMNS <= column_names("work_items")
    assert OWNER_COLUMNS <= column_names("execution_requests")
    assert {
        "id",
        "tenant_id",
        "principal_id",
        "generation",
        "active",
        "version",
        "created_at",
        "updated_at",
    } == column_names("personal_claude_enrollments")

    # Existing rows keep every value, become service rows, and take their
    # agent's tenant; no historical owner is inferred.
    for table, ids in (("work_items", seeded["items"]), ("execution_requests", seeded["requests"])):
        for row_id, tenant in ids.items():
            after = _row(table, row_id)
            assert {key: after[key] for key in before[row_id]} == before[row_id]
            assert (
                after["tenant_id"],
                after["model_auth_mode"],
                after["owner_principal_id"],
                after["enrollment_generation"],
            ) == (tenant, "service", None, None), (table, row_id)

    _probe_work_item_tuple(seeded)
    _probe_request_tuple(seeded)
    _probe_enrollments()
    _probe_downgrade_guard(config, seeded)


def _probe_work_item_tuple(seeded: dict[str, Any]) -> None:
    tenant = seeded["tenant"]
    other_tenant = _tenant()
    agent = _agent(tenant)
    other_agent = _agent(other_tenant)
    owner = _principal(tenant)
    foreign = _principal(other_tenant)

    # Liveness: a complete personal tuple in the agent's tenant.
    _insert("work_items", _work_item(agent, "7100", **_personal(tenant, owner, 1)))
    # A legacy-shaped insert (an older application inside the window) is a
    # service row with no tenant.
    legacy = _work_item(agent, "7101")
    _insert("work_items", legacy)
    assert (
        _row("work_items", legacy["id"])["model_auth_mode"],
        _row("work_items", legacy["id"])["owner_principal_id"],
        _row("work_items", legacy["id"])["enrollment_generation"],
    ) == ("service", None, None)

    # An incomplete personal tuple.
    _refused("work_items", _work_item(agent, "7110", **_personal(tenant, None, 1)))
    _refused("work_items", _work_item(agent, "7111", **_personal(None, owner, 1)))
    # The generation omitted, then named as an explicit NULL.
    omitted = _personal(tenant, owner, 1)
    del omitted["enrollment_generation"]
    _refused("work_items", _work_item(agent, "7112", **omitted))
    _refused("work_items", _work_item(agent, "7113", **_personal(tenant, owner, None)))
    # A non-positive generation.
    _refused("work_items", _work_item(agent, "7114", **_personal(tenant, owner, 0)))
    # A service row naming an owner or a generation.
    _refused(
        "work_items",
        _work_item(
            agent,
            "7115",
            tenant_id=tenant,
            model_auth_mode="service",
            owner_principal_id=owner,
        ),
    )
    _refused(
        "work_items",
        _work_item(
            agent, "7116", tenant_id=tenant, model_auth_mode="service", enrollment_generation=1
        ),
    )
    # An unknown mode.
    _refused(
        "work_items", _work_item(agent, "7117", tenant_id=tenant, model_auth_mode="subscription")
    )
    # A principal of another tenant; liveness above is the same row with the
    # tenant's own principal.
    _refused("work_items", _work_item(agent, "7118", **_personal(tenant, foreign, 1)))
    # An agent of another tenant: the WorkItem's tenant must be its agent's.
    _refused(
        "work_items",
        _work_item(agent, "7119", tenant_id=other_tenant, model_auth_mode="service"),
    )
    _insert(
        "work_items",
        _work_item(other_agent, "7120", tenant_id=other_tenant, model_auth_mode="service"),
    )
    _refused(
        "work_items",
        _work_item(other_agent, "7121", **_personal(other_tenant, owner, 1)),
    )
    _insert("work_items", _work_item(other_agent, "7122", **_personal(other_tenant, foreign, 1)))


def _probe_request_tuple(seeded: dict[str, Any]) -> None:
    tenant = seeded["tenant"]
    other_tenant = _tenant()
    agent = _agent(tenant)
    owner = _principal(tenant)
    peer = _principal(tenant)

    personal = _work_item(agent, "7200", **_personal(tenant, owner, 1))
    _insert("work_items", personal)
    service = _work_item(agent, "7201", tenant_id=tenant, model_auth_mode="service")
    _insert("work_items", service)

    # Liveness: the WorkItem's own tuple snapshotted on its request.
    _insert("execution_requests", _queued(personal["id"], 1, **_personal(tenant, owner, 1)))
    # A personal request whose owner differs from its WorkItem's.
    _refused("execution_requests", _queued(personal["id"], 2, **_personal(tenant, peer, 1)))
    # An incomplete personal snapshot: the generation omitted, then an explicit NULL.
    omitted = _personal(tenant, owner, 1)
    del omitted["enrollment_generation"]
    _refused("execution_requests", _queued(personal["id"], 3, **omitted))
    _refused("execution_requests", _queued(personal["id"], 4, **_personal(tenant, owner, None)))
    _refused("execution_requests", _queued(personal["id"], 5, **_personal(tenant, None, 1)))
    # A legacy-shaped insert names no mode, owner or generation: accepted under
    # a service WorkItem, refused under a personal one.
    legacy = _queued(service["id"], 1)
    _insert("execution_requests", legacy)
    assert _row("execution_requests", legacy["id"])["model_auth_mode"] == "service"
    _refused("execution_requests", _queued(personal["id"], 6))
    # A service request carrying another tenant than its WorkItem's.
    _refused(
        "execution_requests",
        _queued(service["id"], 2, tenant_id=other_tenant, model_auth_mode="service"),
    )
    _insert(
        "execution_requests",
        _queued(service["id"], 3, tenant_id=tenant, model_auth_mode="service"),
    )
    # A personal request under a service WorkItem.
    _refused("execution_requests", _queued(service["id"], 4, **_personal(tenant, owner, 1)))

    # The WorkItem's mode cannot change while requests reference it; a
    # WorkItem with no request can still be written.
    switch = (
        "UPDATE curie.work_items SET model_auth_mode = 'personal', "
        "owner_principal_id = :owner, enrollment_generation = 1 WHERE id = :id"
    )
    _update_refused(switch, {"owner": owner, "id": service["id"]})
    assert _row("work_items", service["id"])["model_auth_mode"] == "service"
    empty = _work_item(agent, "7202", tenant_id=tenant, model_auth_mode="service")
    _insert("work_items", empty)
    sql_rows(switch, {"owner": owner, "id": empty["id"]})
    assert _row("work_items", empty["id"])["model_auth_mode"] == "personal"

    # The owner cannot change while requests reference it; the bound
    # generation can (the revalidation operation rebinds it).
    _update_refused(
        "UPDATE curie.work_items SET owner_principal_id = :peer WHERE id = :id",
        {"peer": peer, "id": personal["id"]},
    )
    sql_rows(
        "UPDATE curie.work_items SET enrollment_generation = 2 WHERE id = :id",
        {"id": personal["id"]},
    )
    after = _row("work_items", personal["id"])
    assert (after["owner_principal_id"], after["enrollment_generation"]) == (owner, 2)


def _probe_enrollments() -> None:
    tenant = _tenant()
    other_tenant = _tenant()
    owner = _principal(tenant)
    peer = _principal(tenant)
    foreign = _principal(other_tenant)

    _insert("personal_claude_enrollments", _enrollment(tenant, owner))
    # One enrollment per (tenant, principal).
    _refused("personal_claude_enrollments", _enrollment(tenant, owner, generation=2))
    _insert("personal_claude_enrollments", _enrollment(tenant, peer, generation=2, version=3))
    # Non-positive generation and version.
    _refused("personal_claude_enrollments", _enrollment(other_tenant, foreign, generation=0))
    _refused("personal_claude_enrollments", _enrollment(other_tenant, foreign, generation=-1))
    _refused("personal_claude_enrollments", _enrollment(other_tenant, foreign, version=0))
    # A principal of another tenant.
    _refused("personal_claude_enrollments", _enrollment(tenant, foreign))
    _insert("personal_claude_enrollments", _enrollment(other_tenant, foreign))
    defaults = _row("personal_claude_enrollments", _seed_default_enrollment(other_tenant))
    assert (defaults["active"], defaults["version"]) == (True, 1)
    assert defaults["created_at"] is not None and defaults["updated_at"] is not None


def _seed_default_enrollment(tenant_id: UUID) -> UUID:
    """An enrollment naming only its key and generation takes the column defaults."""

    enrollment_id = uuid.uuid4()
    _insert(
        "personal_claude_enrollments",
        {
            "id": enrollment_id,
            "tenant_id": tenant_id,
            "principal_id": _principal(tenant_id),
            "generation": 1,
        },
    )
    return enrollment_id


def _probe_downgrade_guard(config: Any, seeded: dict[str, Any]) -> None:
    def refused() -> None:
        with pytest.raises(RuntimeError, match="personal_execution_records_present"):
            command.downgrade(config, BELOW)
        assert stamped_revision() == REVISION
        assert "personal_claude_enrollments" in _tables()
        assert OWNER_COLUMNS <= column_names("work_items")

    # Personal WorkItems (with personal requests) and enrollments remain.
    refused()
    sql_rows("DELETE FROM curie.work_items WHERE model_auth_mode = 'personal'")
    assert sql_dicts(
        "SELECT count(*) AS n FROM curie.execution_requests WHERE model_auth_mode = 'personal'"
    ) == [{"n": 0}]
    # Enrollments alone still hold the downgrade.
    refused()
    sql_rows("DELETE FROM curie.personal_claude_enrollments")

    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    assert "personal_claude_enrollments" not in _tables()
    assert not OWNER_COLUMNS & column_names("work_items")
    assert not OWNER_COLUMNS & column_names("execution_requests")
    # The service rows that existed before the upgrade survive the round trip.
    for row_id in seeded["items"]:
        assert _row("work_items", row_id)["id"] == row_id
    for row_id in seeded["requests"]:
        assert _row("execution_requests", row_id)["id"] == row_id

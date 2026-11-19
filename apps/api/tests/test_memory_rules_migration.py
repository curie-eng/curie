"""Migration 0185 stores learned memory rules, their audit log and the work item pins (#5795).

Real Postgres. The upgrade is driven through Alembic from the revision below it
with rows an earlier release wrote, so the defaults they read are the ones an
existing install sees.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_rows,
    stamped_revision,
)
from alembic import command
from sqlalchemy.exc import DBAPIError, IntegrityError
from test_requester_coding_migration import _insert, _owners

REVISION = "0185"
BELOW = "0184"
STAMP = datetime(2026, 11, 19, 12, 0, tzinfo=UTC)
DIGEST = "ab" * 32


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _agent(tenant: uuid.UUID | None = None) -> uuid.UUID:
    agent_id = uuid.uuid4()
    values: dict[str, Any] = {"id": agent_id, "name": f"m0185-{agent_id.hex[:8]}"}
    if tenant is not None:
        values["tenant_id"] = tenant
    _insert("agents", values)
    return agent_id


def _work_item(agent_id: uuid.UUID, issue: int) -> tuple[uuid.UUID, uuid.UUID]:
    """A WorkItem and its request exactly as the release below writes them."""

    item_id, request_id = uuid.uuid4(), uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.work_items "
        "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "code_host_kind, code_host_host, repository_project_id, "
        "code_host_installation_id, agent_id, repository_path, conversation_id, "
        "version, next_sequence, created_at, updated_at) VALUES "
        "(:id, 'github', 'github.com', '101', :issue, 'github', 'github.com', "
        "'101', 202, :agent, 'acme-corp/acme-bot', :conversation, 1, 2, :stamp, :stamp)",
        {
            "id": item_id,
            "issue": str(issue),
            "agent": agent_id,
            "conversation": f"m0185-{item_id.hex[:8]}",
            "stamp": STAMP,
        },
    )
    _insert(
        "execution_requests",
        {"id": request_id, "work_item_id": item_id, "sequence": 1, "status": "queued"},
    )
    return item_id, request_id


def _lineage(agent_id: uuid.UUID) -> uuid.UUID:
    version_id, deployment_id, lineage_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    _insert(
        "agent_versions",
        {"id": version_id, "agent_id": agent_id, "version_label": "v1", "created_by": "fixture"},
    )
    sql_rows(
        "INSERT INTO curie.deployments (id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent, :version, CAST('dev' AS curie.environment), 'active')",
        {"id": deployment_id, "agent": agent_id, "version": version_id},
    )
    _insert(
        "thread_publication_lineages",
        {
            "id": lineage_id,
            "agent_id": agent_id,
            "deployment_id": deployment_id,
            "conversation_id": f"m0185-{lineage_id.hex[:8]}",
            "repo_full_name": "acme-corp/acme-bot",
            "base_sha": "a" * 40,
            "branch": f"curie/publication-{lineage_id.hex}",
        },
    )
    return lineage_id


def _rule(tenant: uuid.UUID, agent: uuid.UUID, **changes: Any) -> dict[str, Any]:
    text = changes.pop("text", "Prefer small functions.")
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": tenant,
        "agent_id": agent,
        "scope_kind": "repository",
        "scope_key": "github:github.com:101",
        "scope_label": "acme-corp/acme-bot",
        "number": 1,
        "state": "proposed",
        "text": text,
        "sha256": _sha(text),
        "format_version": 1,
        "reason": "The reviewer stated a general rule.",
        "proposed_by": "reviewer-example",
        "proposed_at": STAMP,
    }
    values.update(changes)
    return values


def _accepted(principal: uuid.UUID, **changes: Any) -> dict[str, Any]:
    return {"state": "accepted", "accepted_by": principal, "accepted_at": STAMP, **changes}


def _owner() -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    """A tenant with one principal and an agent of that tenant."""

    tenant, principal, _ = _owners()
    return tenant, principal, _agent(tenant)


def _names(table: str) -> set[str]:
    return {
        row[0]
        for row in sql_rows(
            "SELECT conname FROM pg_constraint WHERE conrelid = CAST(:table AS regclass)",
            {"table": f"curie.{table}"},
        )
    }


def _indexes(table: str) -> dict[str, str]:
    return {
        row[0]: row[1]
        for row in sql_rows(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = 'curie' AND tablename = :table",
            {"table": table},
        )
    }


def _default(table: str, column: str) -> tuple[str, str | None]:
    row = sql_rows(
        "SELECT is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :table AND column_name = :column",
        {"table": table, "column": column},
    )[0]
    return str(row[0]), row[1]


def test_upgrade_downgrade_upgrade_and_every_object_exists(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    assert "memory_rules" not in {
        row[0] for row in sql_rows("SELECT tablename FROM pg_tables WHERE schemaname = 'curie'")
    }
    config = alembic_config()
    command.upgrade(config, REVISION)
    command.downgrade(config, "-1")
    assert stamped_revision() == BELOW
    assert "memory_rule_proposals" not in column_names("agents")
    assert "memory_rules_state" not in column_names("work_items")
    command.upgrade(config, "head")
    assert stamped_revision() == REVISION

    assert _default("agents", "memory_rule_proposals") == ("NO", "false")
    assert _default("work_items", "memory_rules_state") == ("NO", "'absent'::text")
    assert _default("work_items", "memory_rules_count") == ("NO", "0")
    assert _default("work_items", "memory_rules_sha256")[0] == "YES"
    assert _default("work_items", "memory_rules_format_version")[0] == "YES"
    assert _default("execution_requests", "memory_rules_evidence") == ("NO", "'none'::text")
    assert _default("memory_rules", "version") == ("NO", "1")
    assert {
        "memory_rules_scope_ck",
        "memory_rules_state_ck",
        "memory_rules_number_ck",
        "memory_rules_text_ck",
        "memory_rules_sha256_ck",
        "memory_rules_format_version_ck",
        "memory_rules_reason_ck",
        "memory_rules_proposed_by_ck",
        "memory_rules_source_ck",
        "memory_rules_acceptance_ck",
        "memory_rules_version_ck",
        "memory_rules_source_request_key",
        "memory_rules_source_turn_key",
        "memory_rules_tenant_agent_fkey",
        "memory_rules_source_request_fkey",
        "memory_rules_source_lineage_fkey",
        "memory_rules_accepted_by_fkey",
        "memory_rules_rejected_by_fkey",
        "memory_rules_removed_by_fkey",
    } <= _names("memory_rules")
    indexes = _indexes("memory_rules")
    assert "(agent_id, scope_kind, scope_key, number)" in indexes["memory_rules_scope_number_key"]
    assert "WHERE (number IS NOT NULL)" in indexes["memory_rules_scope_number_key"]
    assert "(source_lineage_id)" in indexes["memory_rules_source_lineage_key"]
    assert "(agent_id, scope_kind, scope_key, state)" in indexes["ix_memory_rules_scope_state"]
    assert {
        "memory_rule_events_event_ck",
        "memory_rule_events_state_ck",
        "memory_rule_events_actor_ck",
        "memory_rule_events_rule_fkey",
        "memory_rule_events_principal_fkey",
    } <= _names("memory_rule_events")
    assert {"work_items_memory_rules_ck"} <= _names("work_items")
    assert {
        "work_item_memory_rule_snapshots_numbers_ck",
        "work_item_memory_rule_snapshots_text_ck",
        "work_item_memory_rule_snapshots_sha256_ck",
    } <= _names("work_item_memory_rule_snapshots")
    assert "execution_requests_memory_rules_evidence_ck" in _names("execution_requests")
    identity = sql_rows(
        "SELECT is_identity FROM information_schema.columns WHERE table_schema = 'curie' "
        "AND table_name = 'memory_rule_events' AND column_name = 'id'"
    )[0][0]
    assert identity == "YES"


def test_existing_rows_read_absent_and_none_after_the_upgrade(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    agent = _agent()
    item_id, request_id = _work_item(agent, 1)

    command.upgrade(alembic_config(), REVISION)

    pin = sql_rows(
        "SELECT memory_rules_state, memory_rules_count, memory_rules_sha256, "
        "memory_rules_format_version FROM curie.work_items WHERE id = :id",
        {"id": item_id},
    )[0]
    assert tuple(pin) == ("absent", 0, None, None)
    evidence = sql_rows(
        "SELECT memory_rules_evidence FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )[0][0]
    assert evidence == "none"
    switch = sql_rows(
        "SELECT memory_rule_proposals FROM curie.agents WHERE id = :id", {"id": agent}
    )[0][0]
    assert switch is False
    # The release below keeps working: its inserts name none of the new columns.
    _work_item(agent, 2)


def test_a_valid_rule_of_every_state_and_its_audit_rows_are_stored(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    tenant, principal, agent = _owner()
    _, request = _work_item(agent, 3)
    _insert(
        "memory_rules",
        _rule(tenant, agent, number=None, state="candidate", source_request_id=request),
    )
    proposed = _rule(tenant, agent, number=2)
    _insert("memory_rules", proposed)
    _insert("memory_rules", _rule(tenant, agent, **_accepted(principal), number=3))
    _insert(
        "memory_rules",
        _rule(tenant, agent, number=4, state="rejected", rejected_by=principal, rejected_at=STAMP),
    )
    _insert(
        "memory_rules",
        _rule(
            tenant,
            agent,
            **_accepted(principal, state="removed"),
            number=5,
            removed_by=principal,
            removed_at=STAMP,
        ),
    )
    _insert(
        "memory_rules",
        _rule(
            tenant,
            agent,
            number=None,
            state="superseded",
            source_request_id=None,
            source_turn="turn-1",
        ),
    )
    _insert("memory_rules", _rule(tenant, agent, number=None, state="discarded"))
    # A 240 byte rule of multibyte characters is stored; the bound is bytes.
    multibyte = "é" * 120
    _insert("memory_rules", _rule(tenant, agent, number=6, text=multibyte))
    for event in (
        {
            "event": "propose",
            "from_state": None,
            "to_state": "proposed",
            "actor_kind": "agent",
            "actor": "runner",
        },
        {
            "event": "accept",
            "from_state": "proposed",
            "to_state": "accepted",
            "actor_kind": "principal",
            "actor": "person",
            "principal_id": principal,
        },
    ):
        _insert(
            "memory_rule_events",
            {"rule_id": proposed["id"], "tenant_id": tenant, "agent_id": agent, **event},
        )
    assert sql_rows("SELECT count(*) FROM curie.memory_rule_events")[0][0] == 2
    # Rules and events go with their agent.
    sql_rows("DELETE FROM curie.agents WHERE id = :id", {"id": agent})
    assert sql_rows("SELECT count(*) FROM curie.memory_rules")[0][0] == 0
    assert sql_rows("SELECT count(*) FROM curie.memory_rule_events")[0][0] == 0


def _refused(constraint: str, table: str, values: dict[str, Any]) -> None:
    with pytest.raises(IntegrityError, match=constraint):
        _insert(table, values)


def test_the_database_refuses_each_invalid_rule(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    tenant, principal, agent = _owner()
    _insert("memory_rules", _rule(tenant, agent, number=1))

    _refused("memory_rules_text_ck", "memory_rules", _rule(tenant, agent, number=2, text="x" * 241))
    _refused("memory_rules_text_ck", "memory_rules", _rule(tenant, agent, number=2, text="é" * 121))
    _refused("memory_rules_text_ck", "memory_rules", _rule(tenant, agent, number=2, text=""))
    _refused(
        "memory_rules_text_ck", "memory_rules", _rule(tenant, agent, number=2, text="one\rtwo")
    )
    _refused(
        "memory_rules_text_ck", "memory_rules", _rule(tenant, agent, number=2, text="one\ntwo")
    )
    _refused(
        "memory_rules_reason_ck", "memory_rules", _rule(tenant, agent, number=2, reason="one\ntwo")
    )
    _refused(
        "memory_rules_reason_ck", "memory_rules", _rule(tenant, agent, number=2, reason="r" * 241)
    )
    _refused(
        "memory_rules_sha256_ck", "memory_rules", _rule(tenant, agent, number=2, sha256=DIGEST)
    )
    _refused(
        "memory_rules_format_version_ck",
        "memory_rules",
        _rule(tenant, agent, number=2, format_version=2),
    )
    _refused("memory_rules_state_ck", "memory_rules", _rule(tenant, agent, number=2, state="live"))
    _refused(
        "memory_rules_scope_ck", "memory_rules", _rule(tenant, agent, number=2, scope_kind="team")
    )
    _refused(
        "memory_rules_scope_ck", "memory_rules", _rule(tenant, agent, number=2, scope_kind="agent")
    )
    _refused("memory_rules_number_ck", "memory_rules", _rule(tenant, agent, number=None))
    _refused(
        "memory_rules_number_ck", "memory_rules", _rule(tenant, agent, number=2, state="discarded")
    )
    _refused("memory_rules_number_ck", "memory_rules", _rule(tenant, agent, number=0))
    # A candidate needs its source request; a row never carries both sources.
    _refused(
        "memory_rules_source_ck",
        "memory_rules",
        _rule(tenant, agent, number=None, state="candidate"),
    )
    _, request = _work_item(agent, 4)
    _refused(
        "memory_rules_source_ck",
        "memory_rules",
        _rule(tenant, agent, number=2, source_request_id=request, source_turn="turn-1"),
    )
    # Acceptance is recorded exactly when the state says so.
    _refused(
        "memory_rules_acceptance_ck",
        "memory_rules",
        _rule(tenant, agent, number=2, state="accepted"),
    )
    _refused(
        "memory_rules_acceptance_ck",
        "memory_rules",
        _rule(tenant, agent, number=2, state="accepted", accepted_at=STAMP),
    )
    _refused(
        "memory_rules_acceptance_ck",
        "memory_rules",
        _rule(tenant, agent, number=2, accepted_by=principal, accepted_at=STAMP),
    )
    _refused(
        "memory_rules_acceptance_ck",
        "memory_rules",
        _rule(tenant, agent, number=2, state="rejected"),
    )
    _refused(
        "memory_rules_acceptance_ck",
        "memory_rules",
        _rule(tenant, agent, **_accepted(principal, state="removed"), number=2),
    )
    _refused("memory_rules_version_ck", "memory_rules", _rule(tenant, agent, number=2, version=0))
    # The three actors are principals of the rule's own tenant.
    other_tenant, other_principal, _ = _owners()
    _refused(
        "memory_rules_accepted_by_fkey",
        "memory_rules",
        _rule(tenant, agent, **_accepted(other_principal), number=2),
    )
    assert other_tenant != tenant
    _refused(
        "memory_rules_accepted_by_fkey",
        "memory_rules",
        _rule(tenant, agent, **_accepted(uuid.uuid4()), number=2),
    )


def test_the_database_refuses_duplicates(isolated_migration_db: IsolatedMigrationDb) -> None:
    isolated_migration_db.at(REVISION)
    tenant, principal, agent = _owner()
    _insert("memory_rules", _rule(tenant, agent, number=1))
    # Two rules with one number in one scope; another scope or agent may reuse it.
    _refused("memory_rules_scope_number_key", "memory_rules", _rule(tenant, agent, number=1))
    _insert("memory_rules", _rule(tenant, agent, number=1, scope_key="github:github.com:202"))
    _insert("memory_rules", _rule(tenant, _agent(tenant), number=1))
    _insert("memory_rules", _rule(tenant, agent, number=1, scope_kind="agent", scope_key=""))
    # A hidden candidate burns no number: two of them coexist.
    _, first = _work_item(agent, 5)
    _, second = _work_item(agent, 6)
    for request in (first, second):
        _insert(
            "memory_rules",
            _rule(tenant, agent, number=None, state="candidate", source_request_id=request),
        )
    # One rule per execution request, ever.
    _refused(
        "memory_rules_source_request_key",
        "memory_rules",
        _rule(tenant, agent, number=7, source_request_id=first),
    )
    # One rule per runner turn per agent.
    _insert("memory_rules", _rule(tenant, agent, number=8, source_turn="turn-1"))
    _refused(
        "memory_rules_source_turn_key",
        "memory_rules",
        _rule(tenant, agent, number=9, source_turn="turn-1"),
    )
    _insert("memory_rules", _rule(tenant, _agent(tenant), number=2, source_turn="turn-1"))
    # At most one promoted rule per pull request lineage.
    lineage = _lineage(agent)
    _insert("memory_rules", _rule(tenant, agent, number=10, source_lineage_id=lineage))
    for state in ("proposed", "accepted"):
        extra = _accepted(principal) if state == "accepted" else {}
        _refused(
            "memory_rules_source_lineage_key",
            "memory_rules",
            _rule(tenant, agent, number=11, source_lineage_id=lineage, **extra),
        )
    # A discarded candidate does not hold the lineage.
    _insert(
        "memory_rules",
        _rule(tenant, agent, number=None, state="discarded", source_lineage_id=lineage),
    )


def test_the_database_refuses_nul_with_the_driver_character_error(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    tenant, _, agent = _owner()
    with pytest.raises(DBAPIError) as raised:
        _insert("memory_rules", _rule(tenant, agent, number=1, text="one\x00two"))
    assert getattr(raised.value.orig, "sqlstate", None) == "22021"
    assert not isinstance(raised.value, IntegrityError)
    assert sql_rows("SELECT count(*) FROM curie.memory_rules")[0][0] == 0


def test_deleting_a_request_unlinks_a_proposed_rule(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    tenant, _, agent = _owner()
    item, request = _work_item(agent, 7)
    rule = _rule(tenant, agent, number=1, source_request_id=request)
    _insert("memory_rules", rule)
    sql_rows("DELETE FROM curie.work_items WHERE id = :id", {"id": item})
    kept = sql_rows(
        "SELECT source_request_id FROM curie.memory_rules WHERE id = :id", {"id": rule["id"]}
    )
    assert [row[0] for row in kept] == [None]


def test_the_work_item_pin_and_snapshot_checks(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    agent = _agent()
    item, request = _work_item(agent, 8)

    def pin(**values: Any) -> None:
        assignments = ", ".join(f"{name} = :{name}" for name in values)
        sql_rows(
            f"UPDATE curie.work_items SET {assignments} WHERE id = :id", {"id": item, **values}
        )

    selected = {
        "memory_rules_state": "selected",
        "memory_rules_count": 16,
        "memory_rules_sha256": DIGEST,
        "memory_rules_format_version": 1,
    }
    pin(**selected)
    pin(**{**selected, "memory_rules_count": 1})
    for bad in (
        {"memory_rules_count": 17},
        {"memory_rules_count": 0},
        {"memory_rules_sha256": None},
        {"memory_rules_sha256": "AB" * 32},
        {"memory_rules_format_version": 2},
        {"memory_rules_format_version": None},
        {"memory_rules_state": "pending"},
    ):
        with pytest.raises(IntegrityError, match="work_items_memory_rules_ck"):
            pin(**{**selected, **bad})
    for bad in (
        {"memory_rules_state": "absent", "memory_rules_count": 1},
        {"memory_rules_state": "absent", "memory_rules_count": 0},
    ):
        with pytest.raises(IntegrityError, match="work_items_memory_rules_ck"):
            pin(**{**selected, **bad})
    pin(
        memory_rules_state="absent",
        memory_rules_count=0,
        memory_rules_sha256=None,
        memory_rules_format_version=None,
    )

    text = "R1: a\nR2: b"
    snapshot = {"work_item_id": item, "rule_numbers": [1, 2], "text": text, "sha256": _sha(text)}
    sql_rows(
        "INSERT INTO curie.work_item_memory_rule_snapshots "
        "(work_item_id, rule_numbers, text, sha256) VALUES (:work_item_id, :rule_numbers, "
        ":text, :sha256)",
        snapshot,
    )
    sql_rows("DELETE FROM curie.work_item_memory_rule_snapshots")
    big = "x" * 4097
    for bad, constraint in (
        ({"text": big, "sha256": _sha(big)}, "snapshots_text_ck"),
        ({"sha256": DIGEST}, "snapshots_sha256_ck"),
        ({"rule_numbers": []}, "snapshots_numbers_ck"),
        ({"rule_numbers": list(range(1, 18))}, "snapshots_numbers_ck"),
    ):
        with pytest.raises(IntegrityError, match=constraint):
            sql_rows(
                "INSERT INTO curie.work_item_memory_rule_snapshots "
                "(work_item_id, rule_numbers, text, sha256) VALUES (:work_item_id, "
                ":rule_numbers, :text, :sha256)",
                {**snapshot, **bad},
            )
    sql_rows(
        "INSERT INTO curie.work_item_memory_rule_snapshots "
        "(work_item_id, rule_numbers, text, sha256) VALUES (:work_item_id, :rule_numbers, "
        ":text, :sha256)",
        {**snapshot, "text": "x" * 4096, "sha256": _sha("x" * 4096)},
    )
    # The snapshot goes with its WorkItem.
    sql_rows("DELETE FROM curie.work_items WHERE id = :id", {"id": item})
    assert sql_rows("SELECT count(*) FROM curie.work_item_memory_rule_snapshots")[0][0] == 0

    item, request = _work_item(agent, 9)
    sql_rows(
        "UPDATE curie.execution_requests SET memory_rules_evidence = 'applied' WHERE id = :id",
        {"id": request},
    )
    with pytest.raises(IntegrityError, match="execution_requests_memory_rules_evidence_ck"):
        sql_rows(
            "UPDATE curie.execution_requests SET memory_rules_evidence = 'maybe' WHERE id = :id",
            {"id": request},
        )


@pytest.mark.parametrize("held", ["rule", "pin", "evidence"])
def test_downgrade_refuses_while_rule_state_exists(
    isolated_migration_db: IsolatedMigrationDb, held: str
) -> None:
    isolated_migration_db.at(REVISION)
    tenant, _, agent = _owner()
    item, request = _work_item(agent, 10)
    if held == "rule":
        _insert("memory_rules", _rule(tenant, agent))
    elif held == "pin":
        sql_rows(
            "UPDATE curie.work_items SET memory_rules_state = 'selected', "
            "memory_rules_count = 1, memory_rules_sha256 = :d, "
            "memory_rules_format_version = 1 WHERE id = :id",
            {"d": DIGEST, "id": item},
        )
    else:
        sql_rows(
            "UPDATE curie.execution_requests SET memory_rules_evidence = 'pending' WHERE id = :id",
            {"id": request},
        )
    with pytest.raises(Exception, match="memory_rules_present"):
        command.downgrade(alembic_config(), "-1")
    assert stamped_revision() == REVISION
    assert "memory_rules_state" in column_names("work_items")

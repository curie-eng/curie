"""Historical backfill, constraints and snapshot deletion on real Postgres."""

from __future__ import annotations

import uuid

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from sqlalchemy.exc import IntegrityError
from test_requester_coding_migration import _owners


@pytest.mark.parametrize("request_status", ["waiting", "failed"])
def test_upgrade_backfills_historical_without_selecting_current_documents(
    isolated_migration_db: IsolatedMigrationDb,
    request_status: str,
) -> None:
    isolated_migration_db.at("0168")
    tenant, principal, _ = _owners()
    sql_rows(
        "INSERT INTO curie.requester_coding_profiles "
        "(tenant_id, principal_id, text, revision, sha256, format_version) "
        "VALUES (:t, :p, 'current document', 1, "
        "encode(sha256(convert_to('current document', 'UTF8')), 'hex'), 1)",
        {"t": tenant, "p": principal},
    )
    agent = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, tenant_id, name, repo_full_name) "
        "VALUES (:id, :t, :name, 'acme-corp/acme-bot')",
        {"id": agent, "t": tenant, "name": f"acme-migration-{agent}"},
    )
    items = {}
    for mode in ("service", "personal"):
        item, request = uuid.uuid4(), uuid.uuid4()
        items[mode] = item
        params = {
            "id": item,
            "t": tenant,
            "owner": principal if mode == "personal" else None,
            "gen": 1 if mode == "personal" else None,
            "mode": mode,
            "agent": agent,
        }
        sql_rows(
            "INSERT INTO curie.work_items "
            "(id, tenant_id, model_auth_mode, owner_principal_id, enrollment_generation, "
            "tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
            "code_host_kind, code_host_host, repository_project_id, repository_path, "
            "code_host_installation_id, agent_id, conversation_id) "
            "VALUES (:id, :t, :mode, :owner, :gen, 'github', 'github.com', '101', "
            "CASE WHEN :mode = 'personal' THEN '2' ELSE '1' END, 'github', 'github.com', "
            "'101', 'acme-corp/acme-bot', 202, :agent, 'migration-example')",
            params,
        )
        sql_rows(
            "INSERT INTO curie.execution_requests "
            "(id, work_item_id, tenant_id, model_auth_mode, owner_principal_id, "
            "enrollment_generation, sequence, status, terminal_cause, started_at, "
            "execution_deadline, terminal_at, execution_attempts, runtime_epoch, "
            "wait_deadline, requester, "
            "objective, reply_kind, reply_address, reply_conversation_id) "
            "VALUES (:request, :id, :t, :mode, :owner, :gen, 1, :status, :cause, "
            "CASE WHEN :is_failed THEN now() ELSE NULL END, "
            "CASE WHEN :is_failed THEN now() + interval '1 hour' ELSE NULL END, "
            "CASE WHEN :is_failed THEN now() ELSE NULL END, "
            "CASE WHEN :is_failed THEN 1 ELSE 0 END, "
            "CASE WHEN :is_failed THEN 1 ELSE 0 END, "
            "now() + interval '1 hour', :requester, "
            "'Implement the issue', 'slack', 'C0EXAMPLE1', '1700000000.000100')",
            {
                **params,
                "request": request,
                "requester": str(principal),
                "status": request_status,
                "is_failed": request_status == "failed",
                "cause": "ci_timeout" if request_status == "failed" else None,
            },
        )
    command.upgrade(alembic_config(), "0169")
    assert stamped_revision() == "0169"
    rows = sql_dicts(
        "SELECT id, requester_principal_id, coding_instructions_state, "
        "coding_instructions_absence_reason, coding_instructions_revision "
        "FROM curie.work_items ORDER BY tracker_issue_id"
    )
    assert rows == [
        {
            "id": items[mode],
            "requester_principal_id": principal if mode == "personal" else None,
            "coding_instructions_state": "absent",
            "coding_instructions_absence_reason": "historical",
            "coding_instructions_revision": None,
        }
        for mode in ("service", "personal")
    ]
    assert sql_dicts("SELECT * FROM curie.work_item_coding_snapshots") == []
    assert sql_rows("SELECT status, terminal_cause FROM curie.execution_requests") == [
        (request_status, "ci_timeout" if request_status == "failed" else None)
    ] * 2
    assert sql_rows(
        "SELECT bool_and(coding_instructions_selection_id = work_item_id "
        "AND coding_instructions_evidence = 'none') FROM curie.execution_requests"
    ) == [(True,)]
    with pytest.raises(IntegrityError):
        sql_rows("UPDATE curie.work_items SET coding_instructions_state = 'selected'")
    with pytest.raises(IntegrityError):
        sql_rows(
            "UPDATE curie.execution_requests SET coding_instructions_selection_id = :id",
            {"id": uuid.uuid4()},
        )
    sql_rows(
        "INSERT INTO curie.work_item_coding_snapshots (work_item_id, text) "
        "VALUES (:id, 'private snapshot')",
        {"id": items["service"]},
    )
    sql_rows("DELETE FROM curie.work_items WHERE id = :id", {"id": items["service"]})
    assert sql_dicts("SELECT * FROM curie.work_item_coding_snapshots") == []
    command.downgrade(alembic_config(), "0168")
    assert stamped_revision() == "0168"
    command.upgrade(alembic_config(), "0169")
    sql_rows(
        "UPDATE curie.work_items SET coding_instructions_absence_reason = 'application_unavailable'"
    )
    with pytest.raises(RuntimeError, match="requester_instruction_selections_present"):
        command.downgrade(alembic_config(), "0168")
    assert stamped_revision() == "0169"

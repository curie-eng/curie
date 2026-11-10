"""Real migration evidence. @spec AUTOMATED-REMEDIATION-15."""

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

CODE = "http_identity_selected"


def owed_nomination(state: str = "approval_requested") -> uuid.UUID:
    """@spec AUTOMATED-REMEDIATION-15: a synthetic nomination whose approval is owed."""
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": f"synthetic-{agent_id.hex[:8]}"},
    )
    event_id = f"event-{uuid.uuid4().hex[:10]}"
    sql_rows(
        "INSERT INTO curie.remediation_nomination_submissions "
        "(event_id, agent_id, hook, block_sha256) VALUES (:e, :agent, 'alerts', :sha)",
        {"e": event_id, "agent": agent_id, "sha": "cd" * 32},
    )
    nomination_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.remediation_nominations "
        "(id, agent_id, hook, event_id, action, kind, arguments, arguments_sha256, target, "
        "reason, state, approval_reason) VALUES (:id, :agent, 'alerts', :e, 'scale-out-api', "
        "'remediate', '{}', :sha, 'k8s:\"example-api\"', 'example reason', :state, "
        "'not_automatic')",
        {"id": nomination_id, "agent": agent_id, "e": event_id, "sha": "ef" * 32, "state": state},
    )
    return nomination_id


def end_selected(nomination_id: uuid.UUID) -> None:
    """@spec AUTOMATED-REMEDIATION-15."""
    sql_rows(
        "UPDATE curie.remediation_nominations SET state = 'refused', refusal_code = :code "
        "WHERE id = :id",
        {"code": CODE, "id": nomination_id},
    )


def row(nomination_id: uuid.UUID) -> dict:
    """@spec AUTOMATED-REMEDIATION-15."""
    return sql_dicts(
        "SELECT * FROM curie.remediation_nominations WHERE id = :id", {"id": nomination_id}
    )[0]


def test_upgrade_accepts_the_selected_refusal_and_still_refuses_any_other_code(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: the check is widened by exactly one code."""
    isolated_migration_db.at("0137")
    before = owed_nomination()
    with pytest.raises(IntegrityError):
        end_selected(before)
    command.upgrade(alembic_config(), "0138")
    assert stamped_revision() == "0138"
    end_selected(before)
    assert (row(before)["state"], row(before)["refusal_code"]) == ("refused", CODE)
    other = owed_nomination()
    with pytest.raises(IntegrityError):
        sql_rows(
            "UPDATE curie.remediation_nominations SET state = 'refused', "
            "refusal_code = 'synthetic_unknown_code' WHERE id = :id",
            {"id": other},
        )


def test_downgrade_returns_an_ended_nomination_to_the_owed_state(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AUTOMATED-REMEDIATION-15: no record is lost or left violating the narrower check."""
    isolated_migration_db.at("0138")
    ended = owed_nomination()
    end_selected(ended)
    untouched = owed_nomination()
    command.downgrade(alembic_config(), "0137")
    assert stamped_revision() == "0137"
    restored = row(ended)
    assert (restored["state"], restored["refusal_code"]) == ("approval_requested", None)
    assert restored["approval_reason"] == "not_automatic" and restored["approval_id"] is None
    assert row(untouched)["state"] == "approval_requested"
    with pytest.raises(IntegrityError):
        end_selected(ended)

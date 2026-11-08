"""Migration 0153 adds the immutable approval selection columns. @spec REQUEST-APPROVAL-4.

Plan D10: ``selection_mode`` is NOT NULL with default ``ordinary`` and a CHECK
of ``ordinary``/``dynamic``; the selection snapshot columns are all set on a
dynamic row and all NULL on an ordinary one; ``approvals_selection_nonce_key``
is a partial unique index on ``(agent_id, selection_nonce)`` for non-null
nonces. Existing approvals read back ordinary. The downgrade to 0152 drops the
columns and the index and keeps every approval. The revision is an expand.

One isolated database for the whole lifecycle, seeded only with columns that
exist at both revisions; the database itself is the oracle for each refusal.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

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
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError

BELOW = "0152"
REVISION = "0153"
TEXT_COLUMNS = (
    "selection_connector", "selection_tool", "selection_key_id", "selection_nonce",
    "selection_arguments_sha256",
)
JSON_COLUMNS = ("selection_requester", "selected_approvers")
SNAPSHOT = TEXT_COLUMNS + JSON_COLUMNS
COLUMNS = ("selection_mode", *SNAPSHOT)
INDEX = "approvals_selection_nonce_key"
KINDS = Path(__file__).resolve().parents[1] / "src/curie_api/revision_kinds.json"


def revision_ready() -> None:
    """An absent migration is a body assertion, not a setup error."""
    graph = ScriptDirectory.from_config(alembic_config())
    assert REVISION in {r.revision for r in graph.walk_revisions()}, (
        "approval selection migration 0153 is missing"
    )
    assert graph.get_revision(REVISION).down_revision == BELOW


def _agent() -> uuid.UUID:
    agent = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent, "name": f"synthetic-selection-{agent.hex[:8]}"},
    )
    return agent


def _approval(agent: uuid.UUID, **selection: Any) -> uuid.UUID:
    """An approval row; ``selection`` adds snapshot columns (only valid after upgrade)."""
    approval = uuid.uuid4()
    values: dict[str, Any] = {
        "id": approval, "agent_id": agent, "conversation_id": "C0EXAMPLE1:synthetic-thread",
        "author": "U0EXAMPLE1", "summary": "Approve the synthetic operation",
        "reply_kind": "slack", "reply_channel": "C0EXAMPLE1",
        "dedupe_key": "synthetic-" + approval.hex,
    }
    casts = {name: f":{name}" for name in values}
    for name, value in selection.items():
        values[name] = json.dumps(value) if name in JSON_COLUMNS else value
        casts[name] = f"CAST(:{name} AS jsonb)" if name in JSON_COLUMNS else f":{name}"
    sql_rows(
        f"INSERT INTO curie.approvals ({', '.join(casts)}) VALUES ({', '.join(casts.values())})",
        values,
    )
    return approval


def _dynamic(nonce: str) -> dict[str, Any]:
    tenant = str(uuid.uuid4())
    return {
        "selection_mode": "dynamic",
        "selection_requester": {
            "tenant_id": tenant, "issuer": "https://identity.example.test/directory",
            "subject": "SyntheticRequester",
        },
        "selection_connector": "access",
        "selection_tool": "execute",
        "selection_key_id": "selector-example-1",
        "selection_nonce": nonce,
        "selection_arguments_sha256": "ab" * 32,
        "selected_approvers": [{
            "tenant_id": tenant, "issuer": "https://identity.example.test/directory",
            "subject": "SyntheticApproverA",
        }],
    }


def _selection(approval: uuid.UUID) -> dict[str, Any]:
    found = sql_dicts(
        f"SELECT {', '.join(COLUMNS)} FROM curie.approvals WHERE id = :id", {"id": approval},
    )
    assert len(found) == 1, found
    return found[0]


def _columns() -> dict[str, dict[str, Any]]:
    return {
        row["column_name"]: row
        for row in sql_dicts(
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns WHERE table_schema = 'curie' "
            "AND table_name = 'approvals' AND column_name LIKE 'select%'"
        )
    }


def _index() -> str | None:
    found = sql_dicts(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'curie' AND indexname = :name",
        {"name": INDEX},
    )
    return str(found[0]["indexdef"]) if found else None


def test_0153_upgrade_reads_existing_rows_ordinary_and_downgrade_drops_the_snapshot(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec REQUEST-APPROVAL-4: ordinary and dynamic modes stay explicit in storage."""
    revision_ready()
    assert json.loads(KINDS.read_text(encoding="utf-8"))[REVISION] == "expand"
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    agent, other = _agent(), _agent()
    existing = _approval(agent)
    assert set(COLUMNS).isdisjoint(column_names("approvals"))

    command.upgrade(config, REVISION)

    assert stamped_revision() == REVISION
    columns = _columns()
    assert set(columns) == set(COLUMNS)
    assert columns["selection_mode"]["data_type"] == "text"
    assert columns["selection_mode"]["is_nullable"] == "NO"
    assert "ordinary" in str(columns["selection_mode"]["column_default"])
    for name in TEXT_COLUMNS:
        assert (columns[name]["data_type"], columns[name]["is_nullable"]) == ("text", "YES"), name
    for name in JSON_COLUMNS:
        assert (columns[name]["data_type"], columns[name]["is_nullable"]) == ("jsonb", "YES"), name
    assert _selection(existing) == {"selection_mode": "ordinary", **dict.fromkeys(SNAPSHOT)}
    fresh = _approval(agent)
    assert _selection(fresh)["selection_mode"] == "ordinary"

    # The CHECK keeps the two modes explicit; each refusal leaves nothing behind.
    complete = _dynamic("synthetic-nonce-1")
    refusals: list[dict[str, Any]] = [{"selection_mode": "selective"}]
    for missing in SNAPSHOT:
        refusals.append({k: v for k, v in complete.items() if k != missing})
    for present in SNAPSHOT:
        refusals.append({"selection_mode": "ordinary", present: complete[present]})
    before = sql_dicts("SELECT count(*) AS n FROM curie.approvals")[0]["n"]
    for selection in refusals:
        with pytest.raises(IntegrityError):
            _approval(agent, **selection)
    assert sql_dicts("SELECT count(*) AS n FROM curie.approvals")[0]["n"] == before
    first = _approval(agent, **complete)
    assert _selection(first)["selection_mode"] == "dynamic"
    assert _selection(first)["selected_approvers"] == complete["selected_approvers"]

    # The nonce is single use per agent, only among rows that carry one.
    definition = _index()
    assert definition is not None, f"{INDEX} is missing"
    assert "UNIQUE" in definition and "selection_nonce IS NOT NULL" in definition, definition
    with pytest.raises(IntegrityError):
        _approval(agent, **_dynamic("synthetic-nonce-1"))
    _approval(other, **_dynamic("synthetic-nonce-1"))
    _approval(agent, **_dynamic("synthetic-nonce-2"))
    _approval(agent)
    _approval(agent)

    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    assert set(COLUMNS).isdisjoint(column_names("approvals"))
    assert _index() is None
    kept = sql_dicts(
        "SELECT id FROM curie.approvals WHERE id IN (:existing, :fresh, :first)",
        {"existing": existing, "fresh": fresh, "first": first},
    )
    assert {row["id"] for row in kept} == {existing, fresh, first}

"""Migration 0156 gates a dynamic approval's answer. @spec REQUEST-APPROVAL-5.

Revision 0153 (ADR 0271 decision 4) added ``approvals_dynamic_unapproved``: a
BEFORE UPDATE trigger, backed by ``enforce_dynamic_approval_unapproved()``,
that refused every update approving a dynamic row until decision 5 enforced
its selected set at answer time. 0156 (``0156_dynamic_approval_answer_gate.py``)
is an expand that keeps the trigger and replaces only the function body:
approving a dynamic row is refused unless the same transaction set
``curie.approval_selection_enforced`` to ``v1`` with ``set_config(..., true)``.
Only the HTTP answer owner sets it, in its final commit after the selection
checks pass, so an API that predates answer enforcement (after an application
rollback, say) still cannot approve a dynamic row.

Every other 0153 object stays: the mode and snapshot CHECKs, the nonce index,
the selector-flag CHECK and the INSERT route-lock trigger, each proven by the
statement it still refuses. The downgrade restores the 0153 function body
exactly, so approving is refused even with the setting, while a row approved
meanwhile keeps its status (the trigger fires only on updates).

One isolated database for the whole lifecycle; the database is the oracle.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    _engine,
    alembic_config,
    constraint_exists,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

BELOW = "0154"
REVISION = "0156"
MIGRATION_FILE = "0156_dynamic_approval_answer_gate.py"
KINDS = Path(__file__).resolve().parents[1] / "src/curie_api/revision_kinds.json"
GATE_TRIGGER = "approvals_dynamic_unapproved"
GATE_FUNCTION = "enforce_dynamic_approval_unapproved"
GATE_SETTING = "curie.approval_selection_enforced"
GATE_VALUE = "v1"
WRONG_VALUES = ("v2", "V1", "", "v1 ")
ROUTE_LOCK_TRIGGER = "approvals_approver_ticket_route_lock"
ROUTE_LOCK_FUNCTION = "enforce_approver_ticket_route_lock"
KEPT_CONSTRAINTS = (
    "approvals_selection_mode_ck",
    "approvals_selection_snapshot_ck",
    "agents_approver_ticket_requires_requester_identity_ck",
)
NONCE_INDEX = "approvals_selection_nonce_key"
LIVE = "mcp__access__execute"
OWNING = "access_review"
LOOSER = "ops_review"
JSON_COLUMNS = ("selection_requester", "selected_approvers", "granted_arguments")


def revision_ready() -> None:
    """An absent migration is a body assertion, not a setup error. @spec REQUEST-APPROVAL-5."""
    graph = ScriptDirectory.from_config(alembic_config())
    assert REVISION in {r.revision for r in graph.walk_revisions()}, (
        "dynamic approval answer gate migration 0156 is missing"
    )
    script = graph.get_revision(REVISION)
    assert script is not None and script.down_revision == BELOW, (
        "0156 must revise the 0154 reminder cursor revision directly"
    )
    assert Path(script.path).name == MIGRATION_FILE, script.path


def _agent() -> uuid.UUID:
    """@spec REQUEST-APPROVAL-5."""
    agent = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent, "name": f"synthetic-answer-gate-{agent.hex[:8]}"},
    )
    return agent


def _configure(agent: uuid.UUID) -> None:
    """A selector registered on the owning route, flag on. @spec REQUEST-APPROVAL-5."""
    routes = {
        OWNING: {
            "approvers": {"users": ["U0EXAMPLE1"]},
            "approver_ticket": {
                "connector": "access", "tool": "execute", "key_id": "selector-example-1",
            },
            "separation": {"exclude_requester": True},
        },
        LOOSER: {"approvers": {"users": ["U0EXAMPLE2"]}},
    }
    sql_rows(
        "UPDATE curie.agents SET connector_requester_identity = true, "
        "approval_routes = CAST(:routes AS jsonb) WHERE id = :id",
        {"routes": json.dumps(routes), "id": agent},
    )


def _dynamic(nonce: str) -> dict[str, Any]:
    """A dynamic row exactly as the creation owner writes it. @spec REQUEST-APPROVAL-5."""
    tenant = str(uuid.uuid4())
    issuer = "https://identity.example.test/directory"
    return {
        "granted_tool": LIVE, "route": OWNING, "selection_mode": "dynamic",
        "selection_requester": {"tenant_id": tenant, "issuer": issuer,
                                "subject": "SyntheticRequester"},
        "selection_connector": "access", "selection_tool": "execute",
        "selection_key_id": "selector-example-1", "selection_nonce": nonce,
        "selection_arguments_sha256": "ab" * 32,
        "selected_approvers": [{"tenant_id": tenant, "issuer": issuer,
                                "subject": "SyntheticApproverA"}],
    }


def _approval(agent: uuid.UUID, **columns: Any) -> uuid.UUID:
    """One approval row with ``columns``. @spec REQUEST-APPROVAL-5."""
    approval = uuid.uuid4()
    values: dict[str, Any] = {
        "id": approval, "agent_id": agent, "conversation_id": "C0EXAMPLE1:synthetic-thread",
        "author": "U0EXAMPLE1", "summary": "Approve the synthetic operation",
        "reply_kind": "slack", "reply_channel": "C0EXAMPLE1",
        "dedupe_key": "synthetic-" + approval.hex,
    }
    casts = {name: f":{name}" for name in values}
    for name, value in columns.items():
        as_json = name in JSON_COLUMNS
        values[name] = json.dumps(value) if as_json else value
        casts[name] = f"CAST(:{name} AS jsonb)" if as_json else f":{name}"
    sql_rows(
        f"INSERT INTO curie.approvals ({', '.join(casts)}) VALUES ({', '.join(casts.values())})",
        values,
    )
    return approval


def _set_status(approval: uuid.UUID, status: str, *, setting: str | None = None) -> None:
    """@spec REQUEST-APPROVAL-5: the answer owner's status write, in one transaction.

    With ``setting``, the transaction first sets the gate transaction-locally,
    exactly as the enforcing answer owner does in its final commit.
    """

    async def run() -> None:
        async with _engine().begin() as connection:
            if setting is not None:
                await connection.execute(
                    text("SELECT set_config(:name, :value, true)"),
                    {"name": GATE_SETTING, "value": setting},
                )
            await connection.execute(
                text("UPDATE curie.approvals SET status = :status WHERE id = :id"),
                {"status": status, "id": approval},
            )

    asyncio.run(run())


def _status(approval: uuid.UUID) -> str:
    """@spec REQUEST-APPROVAL-5."""
    found = sql_dicts("SELECT status FROM curie.approvals WHERE id = :id", {"id": approval})
    assert len(found) == 1, found
    return str(found[0]["status"])


def _refused_to_approve(approval: uuid.UUID, *, setting: str | None = None) -> None:
    """@spec REQUEST-APPROVAL-5: the gate refuses the approve and the row stays pending."""
    with pytest.raises(IntegrityError):
        _set_status(approval, "approved", setting=setting)
    assert _status(approval) == "pending"


def _trigger_present(name: str) -> bool:
    """@spec REQUEST-APPROVAL-5."""
    return bool(sql_dicts(
        "SELECT t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'curie' AND c.relname = 'approvals' AND t.tgname = :name",
        {"name": name},
    ))


def _function_body(name: str) -> str | None:
    """The function's full definition, or None when absent. @spec REQUEST-APPROVAL-5."""
    found = sql_dicts(
        "SELECT pg_get_functiondef(p.oid) AS body FROM pg_proc p "
        "JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'curie' AND p.proname = :name",
        {"name": name},
    )
    assert len(found) <= 1, found
    return str(found[0]["body"]) if found else None


def _index_present(name: str) -> bool:
    """@spec REQUEST-APPROVAL-5."""
    return bool(sql_dicts(
        "SELECT indexname FROM pg_indexes WHERE schemaname = 'curie' AND indexname = :name",
        {"name": name},
    ))


def _objects_present() -> None:
    """Every 0153 object, the gate trigger and its function included. @spec REQUEST-APPROVAL-5."""
    for name in KEPT_CONSTRAINTS:
        assert constraint_exists(name), name
    assert _index_present(NONCE_INDEX)
    assert _trigger_present(GATE_TRIGGER)
    assert _function_body(GATE_FUNCTION) is not None
    assert _trigger_present(ROUTE_LOCK_TRIGGER)
    assert _function_body(ROUTE_LOCK_FUNCTION) is not None


def test_0156_lets_only_the_enforcing_answer_owner_approve_and_downgrade_restores_0153(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec REQUEST-APPROVAL-5: only a transaction that set the gate approves a dynamic row."""
    revision_ready()
    assert json.loads(KINDS.read_text(encoding="utf-8"))[REVISION] == "expand"
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    agent, plain = _agent(), _agent()
    _configure(agent)
    answered = _approval(agent, **_dynamic("synthetic-nonce-1"))
    pending = _approval(agent, **_dynamic("synthetic-nonce-2"))
    declined = _approval(agent, **_dynamic("synthetic-nonce-4"))
    ordinary = _approval(plain, granted_tool="Bash", route=LOOSER)
    # The premise at 0153: approving a dynamic row is refused, with or without the setting.
    _objects_present()
    below_body = _function_body(GATE_FUNCTION)
    _refused_to_approve(answered)
    _refused_to_approve(answered, setting=GATE_VALUE)

    command.upgrade(config, REVISION)

    assert stamped_revision() == REVISION
    _objects_present()
    assert _function_body(GATE_FUNCTION) != below_body
    # An answer path that never set the gate, or set any other value, is refused.
    _refused_to_approve(answered)
    for value in WRONG_VALUES:
        _refused_to_approve(answered, setting=value)
    # The enforcing owner's transaction approves.
    _set_status(answered, "approved", setting=GATE_VALUE)
    assert _status(answered) == "approved"
    # The setting is transaction-local: the next plain approve is refused again.
    _refused_to_approve(pending)
    # Rejecting a dynamic row and approving an ordinary one never needed the gate.
    _set_status(declined, "rejected")
    _set_status(ordinary, "approved")
    assert (_status(declined), _status(ordinary)) == ("rejected", "approved")
    # Each kept 0153 guard still refuses the statement it exists for.
    incomplete = {k: v for k, v in _dynamic("synthetic-nonce-3").items() if k != "selection_key_id"}
    with pytest.raises(IntegrityError, match="approvals_selection_snapshot_ck"):
        _approval(agent, **incomplete)
    with pytest.raises(IntegrityError, match=NONCE_INDEX):
        _approval(agent, **_dynamic("synthetic-nonce-1"))
    with pytest.raises(IntegrityError, match="approval_route_locked"):
        _approval(agent, granted_tool=LIVE, route=LOOSER)
    with pytest.raises(IntegrityError, match="agents_approver_ticket_requires_requester"):
        sql_rows(
            "UPDATE curie.agents SET connector_requester_identity = false WHERE id = :id",
            {"id": agent},
        )

    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    _objects_present()
    assert _function_body(GATE_FUNCTION) == below_body
    _refused_to_approve(pending, setting=GATE_VALUE)
    _refused_to_approve(pending)
    assert (_status(answered), _status(declined), _status(ordinary)) == (
        "approved", "rejected", "approved",
    )
    # Expiry was never part of the gate.
    _set_status(pending, "expired")
    assert _status(pending) == "expired"

    command.upgrade(config, REVISION)
    assert stamped_revision() == REVISION
    _objects_present()
    assert _function_body(GATE_FUNCTION) != below_body

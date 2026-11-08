"""Migration 0156 lets storage record an answered dynamic approval. @spec REQUEST-APPROVAL-5.

Revision 0153 (ADR 0271 decision 4) added ``approvals_dynamic_unapproved``: a
BEFORE UPDATE trigger, backed by ``enforce_dynamic_approval_unapproved()``,
that refused every update approving a dynamic row until decision 5 enforced
its selected set at answer time. 0156 is the expand that drops only that
interlock. Every other 0153 object stays: the mode and snapshot CHECKs, the
nonce index, the selector-flag CHECK and the INSERT route-lock trigger, each
proven by the statement it still refuses. The downgrade recreates the trigger
and its function, so a pending dynamic row cannot be approved again, while a
row approved meanwhile keeps its status (the trigger fires only on updates).

One isolated database for the whole lifecycle; the database is the oracle.
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
    constraint_exists,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError

BELOW = "0153"
REVISION = "0156"
KINDS = Path(__file__).resolve().parents[1] / "src/curie_api/revision_kinds.json"
INTERLOCK_TRIGGER = "approvals_dynamic_unapproved"
INTERLOCK_FUNCTION = "enforce_dynamic_approval_unapproved"
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
        "dynamic approval answerable migration 0156 is missing"
    )
    below = {r.revision for r in graph.iterate_revisions(REVISION, "base")}
    assert BELOW in below, "0156 must descend from the 0153 selection revision"


def _agent() -> uuid.UUID:
    """@spec REQUEST-APPROVAL-5."""
    agent = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent, "name": f"synthetic-answerable-{agent.hex[:8]}"},
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


def _set_status(approval: uuid.UUID, status: str) -> None:
    """@spec REQUEST-APPROVAL-5: the answer owner's status write, as one statement."""
    sql_rows("UPDATE curie.approvals SET status = :status WHERE id = :id",
             {"status": status, "id": approval})


def _status(approval: uuid.UUID) -> str:
    """@spec REQUEST-APPROVAL-5."""
    found = sql_dicts("SELECT status FROM curie.approvals WHERE id = :id", {"id": approval})
    assert len(found) == 1, found
    return str(found[0]["status"])


def _trigger_present(name: str) -> bool:
    """@spec REQUEST-APPROVAL-5."""
    return bool(sql_dicts(
        "SELECT t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'curie' AND c.relname = 'approvals' AND t.tgname = :name",
        {"name": name},
    ))


def _function_present(name: str) -> bool:
    """@spec REQUEST-APPROVAL-5."""
    return bool(sql_dicts(
        "SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'curie' AND p.proname = :name",
        {"name": name},
    ))


def _index_present(name: str) -> bool:
    """@spec REQUEST-APPROVAL-5."""
    return bool(sql_dicts(
        "SELECT indexname FROM pg_indexes WHERE schemaname = 'curie' AND indexname = :name",
        {"name": name},
    ))


def _interlock_present() -> bool:
    """Both halves of the 0153 answer interlock, never one without the other.

    @spec REQUEST-APPROVAL-5.
    """
    trigger, function = _trigger_present(INTERLOCK_TRIGGER), _function_present(INTERLOCK_FUNCTION)
    assert trigger == function, (trigger, function)
    return trigger


def _kept_objects_present() -> None:
    """Every other 0153 object, by name. @spec REQUEST-APPROVAL-5."""
    for name in KEPT_CONSTRAINTS:
        assert constraint_exists(name), name
    assert _index_present(NONCE_INDEX)
    assert _trigger_present(ROUTE_LOCK_TRIGGER)
    assert _function_present(ROUTE_LOCK_FUNCTION)


def test_0156_drops_only_the_dynamic_answer_interlock_and_downgrade_restores_it(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec REQUEST-APPROVAL-5: storage no longer vetoes an enforced answer; nothing else moves."""
    revision_ready()
    assert json.loads(KINDS.read_text(encoding="utf-8"))[REVISION] == "expand"
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    agent, plain = _agent(), _agent()
    _configure(agent)
    answered = _approval(agent, **_dynamic("synthetic-nonce-1"))
    pending = _approval(agent, **_dynamic("synthetic-nonce-2"))
    ordinary = _approval(plain, granted_tool="Bash", route=LOOSER)
    # The premise at 0153: approving a dynamic row is refused by the interlock.
    assert _interlock_present()
    with pytest.raises(IntegrityError, match="dynamic_selection_unavailable"):
        _set_status(answered, "approved")
    assert _status(answered) == "pending"

    command.upgrade(config, REVISION)

    assert stamped_revision() == REVISION
    assert not _interlock_present()
    _kept_objects_present()
    _set_status(answered, "approved")
    assert _status(answered) == "approved"
    _set_status(ordinary, "approved")
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
    assert _interlock_present()
    _kept_objects_present()
    with pytest.raises(IntegrityError, match="dynamic_selection_unavailable"):
        _set_status(pending, "approved")
    assert (_status(answered), _status(pending), _status(ordinary)) == (
        "approved", "pending", "approved",
    )
    # Expiry was never part of the interlock.
    _set_status(pending, "expired")
    assert _status(pending) == "expired"

    command.upgrade(config, REVISION)
    assert stamped_revision() == REVISION
    assert not _interlock_present()
    _kept_objects_present()

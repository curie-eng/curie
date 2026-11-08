"""Migration 0153 adds the immutable approval selection columns. @spec REQUEST-APPROVAL-4.

Plan D10: ``selection_mode`` is NOT NULL with default ``ordinary`` and a CHECK
of ``ordinary``/``dynamic``; the selection snapshot columns are all set on a
dynamic row and all NULL on an ordinary one; ``approvals_selection_nonce_key``
is a partial unique index on ``(agent_id, selection_nonce)`` for non-null
nonces. Existing approvals read back ordinary. The revision is an expand.

An expand keeps an older API serving it, during a rolling upgrade and after an
application rollback alike, and such a writer names no selection column and
knows nothing of selectors. Its exact statements are replayed here at 0153:
storage refuses each one that would bypass a registration (a looser, missing
or ordinary route for the registered tool, another tool on its route, a
ticket on a route that registers none, a requester flag turned off under a
registration, an approval of a dynamic row) and keeps every neighbor.

The downgrade to 0152 refuses while any dynamic approval or registration
exists, leaving the revision, columns, rows and nonces as they were; without
them it drops the columns, index and guards and keeps every approval.

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
        as_json = name in JSON_COLUMNS or name == "granted_arguments"
        values[name] = json.dumps(value) if as_json else value
        casts[name] = f"CAST(:{name} AS jsonb)" if as_json else f":{name}"
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
    # A dynamic row exists only on a route that registers its pair.
    _configure(agent, _routes(), flag=True)
    _configure(other, _routes(), flag=True)

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
    first = _approval(agent, granted_tool=LIVE, route=OWNING, **complete)
    assert _selection(first)["selection_mode"] == "dynamic"
    assert _selection(first)["selected_approvers"] == complete["selected_approvers"]

    # The nonce is single use per agent, only among rows that carry one.
    definition = _index()
    assert definition is not None, f"{INDEX} is missing"
    assert "UNIQUE" in definition and "selection_nonce IS NOT NULL" in definition, definition
    with pytest.raises(IntegrityError, match="approvals_selection_nonce_key"):
        _approval(agent, **_registered_dynamic("synthetic-nonce-1"))
    _approval(other, **_registered_dynamic("synthetic-nonce-1"))
    _approval(agent, **_registered_dynamic("synthetic-nonce-2"))
    _approval(agent)
    _approval(agent)

    # A downgrade would turn each dynamic row ordinary and free its nonce.
    with pytest.raises(RuntimeError, match=r"3 dynamic approval\(s\) and 2 agent"):
        command.downgrade(config, BELOW)
    assert stamped_revision() == REVISION
    assert set(COLUMNS) <= column_names("approvals")
    assert _selection(first) == complete
    with pytest.raises(IntegrityError, match="approvals_selection_nonce_key"):
        _approval(agent, **_registered_dynamic("synthetic-nonce-1"))

    # An install with no dynamic approval and no registration downgrades and
    # keeps every row.
    sql_rows("DELETE FROM curie.approvals WHERE selection_mode = 'dynamic'")
    _configure(agent, None, flag=True)
    _configure(other, None, flag=True)
    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    assert set(COLUMNS).isdisjoint(column_names("approvals"))
    assert _index() is None
    assert _guards() == set()
    kept = sql_dicts(
        "SELECT id FROM curie.approvals WHERE id IN (:existing, :fresh)",
        {"existing": existing, "fresh": fresh},
    )
    assert {row["id"] for row in kept} == {existing, fresh}


# --- storage holds the selector invariants for an older writer -------------------

LIVE = "mcp__access__execute"
OWNING = "access_review"
LOOSER = "ops_review"
GUARDS = {
    ("agents", "agents_approver_ticket_requires_requester_identity_ck"),
    ("approvals", "approvals_approver_ticket_route_lock"),
    ("approvals", "approvals_dynamic_unapproved"),
}


def _guards() -> set[tuple[str, str]]:
    """The 0153 storage guards present: the agents CHECK and the approvals triggers."""
    checks = sql_dicts(
        "SELECT 'agents' AS t, conname AS name FROM pg_constraint "
        "WHERE conname = 'agents_approver_ticket_requires_requester_identity_ck'"
    )
    triggers = sql_dicts(
        "SELECT 'approvals' AS t, tgname AS name FROM pg_trigger "
        "WHERE tgname IN ('approvals_approver_ticket_route_lock', 'approvals_dynamic_unapproved')"
    )
    return {(row["t"], row["name"]) for row in checks + triggers}


def _routes(*, registered: bool = True, ticket: Any = None) -> dict[str, Any]:
    """A route map binding two routes, with the pair registered on the owning one."""
    owning: dict[str, Any] = {"approvers": {"users": ["U0EXAMPLE1"]}}
    if registered:
        owning["approver_ticket"] = (
            {"connector": "access", "tool": "execute", "key_id": "selector-example-1"}
            if ticket is None else ticket
        )
        owning["separation"] = {"exclude_requester": True}
    return {OWNING: owning, LOOSER: {"approvers": {"users": ["U0EXAMPLE2"]}}}


def _configure(agent: uuid.UUID, routes: Any, *, flag: bool) -> None:
    """An older or newer agent writer: the flag and the route map as one UPDATE."""
    sql_rows(
        "UPDATE curie.agents SET connector_requester_identity = :flag, "
        "approval_routes = CAST(:routes AS jsonb) WHERE id = :id",
        {"flag": flag, "routes": None if routes is None else json.dumps(routes), "id": agent},
    )


def _flag(agent: uuid.UUID) -> bool:
    return bool(sql_dicts(
        "SELECT connector_requester_identity AS f FROM curie.agents WHERE id = :id",
        {"id": agent},
    )[0]["f"])


def _registered_dynamic(nonce: str) -> dict[str, Any]:
    """A dynamic row exactly as the new creation owner writes it for the pair."""
    return {"granted_tool": LIVE, "route": OWNING, **_dynamic(nonce)}


def test_0153_storage_refuses_an_older_writers_selector_bypass(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec REQUEST-APPROVAL-1/2/4: overlap and rollback writers meet the same storage.

    Each refused statement is one an API built before 0153 issues: its
    approval INSERT names no selection column and its flag write is a plain
    UPDATE. The neighbors (the new owner's dynamic row, other tools, other
    agents, expiry, an ordinary approval) still write.
    """
    revision_ready()
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    agent, plain = _agent(), _agent()
    command.upgrade(config, REVISION)
    assert _guards() == GUARDS
    _configure(agent, _routes(), flag=True)
    before = sql_dicts("SELECT count(*) AS n FROM curie.approvals")[0]["n"]

    # Creation, both ways: the registered tool only on its route as a dynamic
    # row for it, its route only for it, and a ticket only on a route that owns one.
    ticket = {"target": "acct-example-1", "approval_ticket": "synthetic-ticket"}
    for code, holder, older in (
        ("approval_route_locked", agent, {"granted_tool": LIVE, "route": LOOSER}),
        ("approval_route_locked", agent, {"granted_tool": LIVE, "route": None}),
        ("approval_route_locked", agent, {"granted_tool": LIVE, "route": ""}),
        ("approval_route_locked", agent,
         {**_registered_dynamic("synthetic-nonce-9"), "route": LOOSER}),
        ("approval_ticket_required", agent, {"granted_tool": LIVE, "route": OWNING}),
        ("approval_ticket_required", agent,
         {**_registered_dynamic("synthetic-nonce-9"), "selection_tool": "read"}),
        ("approval_ticket_route_mismatch", agent,
         {"granted_tool": "mcp__access__read", "route": OWNING}),
        ("approval_ticket_route_mismatch", agent, {"granted_tool": "Bash", "route": OWNING}),
        ("approval_ticket_route_mismatch", agent, {"granted_tool": None, "route": OWNING}),
        ("approval_ticket_unregistered", agent,
         {"granted_tool": "mcp__access__read", "route": LOOSER, "granted_arguments": ticket}),
        ("approval_ticket_unregistered", agent,
         {"granted_tool": "Bash", "route": None, "granted_arguments": {"approval_ticket": None}}),
        ("approval_ticket_unregistered", plain,
         {"granted_tool": LIVE, "route": LOOSER, "granted_arguments": ticket}),
        ("approval_ticket_unregistered", plain,
         {"granted_tool": LIVE, "route": LOOSER, **_dynamic("synthetic-nonce-9")}),
    ):
        with pytest.raises(IntegrityError, match=code):
            _approval(holder, **older)
    with pytest.raises(IntegrityError, match="dynamic_selection_unavailable"):
        _approval(agent, status="approved", **_registered_dynamic("synthetic-nonce-9"))
    assert sql_dicts("SELECT count(*) AS n FROM curie.approvals")[0]["n"] == before
    dynamic = _approval(
        agent, granted_arguments=ticket, **_registered_dynamic("synthetic-nonce-1"),
    )
    for neighbor in (
        {"granted_tool": "mcp__access__read", "route": LOOSER},
        {"granted_tool": f"{LIVE}__extra", "route": LOOSER},
        {"granted_tool": "mcp__plugin_access__execute", "route": LOOSER},
        {"granted_tool": None, "route": None},
        {"granted_tool": "Bash", "route": LOOSER, "granted_arguments": {"target": "x"}},
        # Rows no creation owner writes keep today's routing, as in the application.
        {"granted_tool": "mcp__curie__publish_changes", "route": OWNING,
         "purpose": "publication"},
    ):
        _approval(agent, **neighbor)
    elsewhere = _approval(plain, granted_tool=LIVE, route=LOOSER)

    # Answering: a dynamic row is never approved; expiry and ordinary rows are unchanged.
    with pytest.raises(IntegrityError, match="dynamic_selection_unavailable"):
        sql_rows("UPDATE curie.approvals SET status = 'approved' WHERE id = :id", {"id": dynamic})
    sql_rows("UPDATE curie.approvals SET status = 'approved' WHERE id = :id", {"id": elsewhere})
    sql_rows("UPDATE curie.approvals SET status = 'expired' WHERE id = :id", {"id": dynamic})
    assert sql_dicts(
        "SELECT status FROM curie.approvals WHERE id = :id", {"id": dynamic},
    ) == [{"status": "expired"}]

    # The flag: never off under a registration, and never a registration without it.
    with pytest.raises(IntegrityError, match="agents_approver_ticket_requires_requester"):
        sql_rows(
            "UPDATE curie.agents SET connector_requester_identity = false WHERE id = :id",
            {"id": agent},
        )
    assert _flag(agent) is True
    with pytest.raises(IntegrityError, match="agents_approver_ticket_requires_requester"):
        _configure(plain, _routes(), flag=False)
    with pytest.raises(IntegrityError, match="agents_approver_ticket_requires_requester"):
        _configure(plain, _routes(registered=False) | {"x": {"approver_ticket": None}}, flag=False)
    _configure(plain, _routes(registered=False), flag=False)
    _configure(agent, _routes(registered=False), flag=True)
    sql_rows(
        "UPDATE curie.agents SET connector_requester_identity = false WHERE id = :id",
        {"id": agent},
    )
    assert _flag(agent) is False
    # Without the registration the tool is ordinary again on any route.
    _approval(agent, granted_tool=LIVE, route=LOOSER)


def test_0153_storage_fails_closed_on_a_registration_it_cannot_read(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec REQUEST-APPROVAL-2: no pair to lock, so no insert for the agent, like the API."""
    revision_ready()
    isolated_migration_db.at(BELOW)
    agent, plain = _agent(), _agent()
    command.upgrade(alembic_config(), REVISION)
    for routes in (
        _routes(ticket={}),
        _routes(ticket={"connector": "access"}),
        _routes(ticket={"connector": "access", "tool": 7}),
        _routes(ticket="access"),
        {OWNING: {"approver_ticket": None}},
    ):
        _configure(agent, routes, flag=True)
        with pytest.raises(IntegrityError, match="approver_ticket_state_invalid"):
            _approval(agent, granted_tool="mcp__other__read", route=LOOSER)
    _approval(plain, granted_tool="mcp__other__read", route=LOOSER)


def test_0153_downgrade_refuses_while_a_registration_exists(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec REQUEST-APPROVAL-1: no older release enforces a registration, so none is dropped.

    After the registration is removed the round trip works, and the upgrade
    puts every guard back.
    """
    revision_ready()
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    agent = _agent()
    command.upgrade(config, REVISION)
    _configure(agent, _routes(), flag=True)

    with pytest.raises(RuntimeError, match=r"0 dynamic approval\(s\) and 1 agent"):
        command.downgrade(config, BELOW)
    assert stamped_revision() == REVISION
    assert _guards() == GUARDS
    with pytest.raises(IntegrityError, match="approval_route_locked"):
        _approval(agent, granted_tool=LIVE, route=LOOSER)

    _configure(agent, _routes(registered=False), flag=True)
    command.downgrade(config, BELOW)
    assert stamped_revision() == BELOW
    assert _guards() == set()
    command.upgrade(config, REVISION)
    assert _guards() == GUARDS

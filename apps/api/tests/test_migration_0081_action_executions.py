"""Migration 0085: the execution record, the ledger columns and capabilities.

Realizes the schema half of the connector action executor contract
(docs/superpowers/specs/2026-10-06-connector-action-executor.md):

* ACTION-EXECUTOR-2 adds ``action_executions``: one row per restore, forward
  action or capability probe the platform runs without a model, with check
  constraints on ``kind`` and ``state``, a unique ``idempotency_key`` and a
  partial unique index allowing at most one restore that is not ``refused`` per
  recorded action.
* ACTION-EXECUTOR-11 adds ``post_version``, ``connector``, ``connector_digest``,
  ``authority_kind`` and ``authority_ref`` to ``agent_actions``. Rows written
  before the change are neither migrated nor purged.
* ACTION-EXECUTOR-13 adds ``connector_capabilities``, keyed on the agent,
  connector and image digest.

The next-only execution migration follows the canvas migration at 0084,
after the immutable published stable chain through 0081.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_dicts,
    sql_rows,
)
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError

REVISION = "0085"
BELOW = "0084"

NEW_ACTION_COLUMNS = {
    "post_version",
    "connector",
    "connector_digest",
    "authority_kind",
    "authority_ref",
}

# ACTION-EXECUTOR-2's column table, verbatim.
EXECUTION_COLUMNS = {
    "id",
    "kind",
    "agent_id",
    "connector",
    "tool",
    "subject_action_id",
    "arguments_sha256",
    "forward_arguments",
    "connector_digest",
    "authority_kind",
    "authority_ref",
    "requested_by",
    "idempotency_key",
    "state",
    "refusal_code",
    "failure_code",
    "attempt",
    "lease_owner",
    "lease_expires_at",
    "dispatched_at",
    "finished_at",
    "created_at",
    "outcome",
}

# ACTION-EXECUTOR-13's stored tuple.
CAPABILITY_COLUMNS = {"agent_id", "connector", "digest", "restore_capable", "observed_at"}

STATES = (
    "requested",
    "claimed",
    "dispatched",
    "confirmed",
    "failed",
    "indeterminate",
    "refused",
)
LIVE_STATES = tuple(state for state in STATES if state != "refused")

DIGEST = "sha256:" + "ab" * 32
# What a pre-change worker recorded: the cleartext snapshot ADR 0124 retires.
LEGACY_PRIOR = {"spec": {"replicas": 3}}
LEGACY_POST = {"spec": {"replicas": 10}}
LEGACY_TARGET = {"kind": "Deployment", "namespace": "public", "name": "api"}

# Every column a pre-change ``agent_actions`` row carries, read back to prove
# the round trip leaves it byte-for-byte as it was.
LEGACY_ROW_COLUMNS = (
    "id, agent_id, conversation_id, call_id, tool, arguments, result, prior_state, "
    "post_state, target, detail, gate_approval_id, status, dedupe_key, created_at, "
    "completed_at, undone_at, undone_by"
)


def _require_revision() -> None:
    """The migration this file tests exists and sits directly on 0084."""

    script = ScriptDirectory.from_config(alembic_config())
    known = {rev.revision for rev in script.walk_revisions()}
    assert REVISION in known, (
        f"no alembic revision {REVISION} adds action_executions (ACTION-EXECUTOR-2) yet"
    )
    revision = script.get_revision(REVISION)
    assert revision is not None
    assert revision.down_revision == BELOW


def _table_exists(table: str) -> bool:
    return bool(
        sql_rows(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'curie' AND table_name = :t",
            {"t": table},
        )
    )


def _insert_agent(name: str = "restorer-agent") -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": name},
    )
    return agent_id


def _insert_legacy_action(
    agent_id: uuid.UUID | None, *, call_id: str = "toolu_legacy"
) -> uuid.UUID:
    """A succeeded row with a cleartext snapshot, as written before this change."""

    action_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agent_actions "
        "(id, agent_id, conversation_id, call_id, tool, arguments, result, "
        "prior_state, post_state, target, detail, status, dedupe_key, completed_at) "
        "VALUES (:id, :agent_id, 'C1', :call_id, 'mcp__k8s__scale', "
        "CAST(:arguments AS jsonb), CAST(:result AS jsonb), CAST(:prior AS jsonb), "
        "CAST(:post AS jsonb), CAST(:target AS jsonb), 'scaled', 'succeeded', :key, now())",
        {
            "id": action_id,
            "agent_id": agent_id,
            "call_id": call_id,
            "arguments": json.dumps({"name": "api", "replicas": 10}),
            "result": json.dumps({"ok": True, "prior": LEGACY_PRIOR}),
            "prior": json.dumps(LEGACY_PRIOR),
            "post": json.dumps(LEGACY_POST),
            "target": json.dumps(LEGACY_TARGET),
            "key": f"event-legacy:{call_id}",
        },
    )
    return action_id


def _legacy_rows() -> list[dict[str, Any]]:
    return sql_dicts(f"SELECT {LEGACY_ROW_COLUMNS} FROM curie.agent_actions ORDER BY call_id")


# Each producer's authority kind (ACTION-EXECUTOR-1), so a row is never shaped
# like something no producer writes.
_AUTHORITY = {"restore": "undo_ruling", "forward": "policy", "probe": "capability_probe"}


def _insert_execution(
    agent_id: uuid.UUID,
    *,
    kind: str = "restore",
    state: str = "requested",
    subject_action_id: uuid.UUID | None = None,
    idempotency_key: str | None = None,
    tool: str | None = "restore",
) -> uuid.UUID:
    execution_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.action_executions "
        "(id, kind, agent_id, connector, tool, subject_action_id, connector_digest, "
        "authority_kind, authority_ref, requested_by, idempotency_key, state, attempt, "
        "created_at) "
        "VALUES (:id, :kind, :agent_id, 'k8s', :tool, :subject, :digest, "
        ":authority_kind, :ref, 'U-operator', :key, :state, 0, now())",
        {
            "id": execution_id,
            "kind": kind,
            "authority_kind": _AUTHORITY.get(kind, "undo_ruling"),
            "agent_id": agent_id,
            "tool": tool,
            "subject": subject_action_id,
            "digest": DIGEST,
            "ref": str(uuid.uuid4()),
            "key": idempotency_key or f"{kind}:{uuid.uuid4()}",
            "state": state,
        },
    )
    return execution_id


def _insert_capability(
    agent_id: uuid.UUID, *, connector: str = "k8s", digest: str = DIGEST
) -> None:
    sql_rows(
        "INSERT INTO curie.connector_capabilities "
        "(agent_id, connector, digest, restore_capable, observed_at) "
        "VALUES (:agent_id, :connector, :digest, true, now())",
        {"agent_id": agent_id, "connector": connector, "digest": digest},
    )


def test_0085_revises_0084() -> None:
    """@spec ACTION-EXECUTOR-2: one additive, hand-written revision on the current head."""

    _require_revision()


def test_the_upgrade_creates_the_execution_and_capability_tables(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2 @spec ACTION-EXECUTOR-11 @spec ACTION-EXECUTOR-13"""

    _require_revision()
    isolated_migration_db.at(REVISION)

    assert _table_exists("action_executions")
    assert column_names("action_executions") == EXECUTION_COLUMNS
    assert NEW_ACTION_COLUMNS <= column_names("agent_actions")
    assert _table_exists("connector_capabilities")
    assert CAPABILITY_COLUMNS <= column_names("connector_capabilities")


def test_existing_ledger_rows_survive_the_round_trip_unchanged(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2 @spec ACTION-EXECUTOR-11

    Real Postgres upgrade, downgrade and upgrade. A pre-change row, cleartext
    snapshot included, is neither migrated nor purged: every column it had reads
    back identical at each step and the new columns are NULL on it.
    """

    _require_revision()
    cfg = alembic_config()
    isolated_migration_db.at(BELOW)
    agent_id = _insert_agent()
    _insert_legacy_action(agent_id, call_id="toolu_a")
    _insert_legacy_action(agent_id, call_id="toolu_b")
    before = _legacy_rows()
    assert len(before) == 2

    command.upgrade(cfg, REVISION)
    assert _legacy_rows() == before
    new_columns = sql_dicts(
        "SELECT post_version, connector, connector_digest, authority_kind, authority_ref "
        "FROM curie.agent_actions"
    )
    assert new_columns == [dict.fromkeys(NEW_ACTION_COLUMNS)] * 2

    command.downgrade(cfg, BELOW)
    assert not _table_exists("action_executions")
    assert not _table_exists("connector_capabilities")
    assert not (NEW_ACTION_COLUMNS & column_names("agent_actions"))
    assert _legacy_rows() == before

    command.upgrade(cfg, REVISION)
    assert _table_exists("action_executions")
    assert NEW_ACTION_COLUMNS <= column_names("agent_actions")
    assert _legacy_rows() == before


def test_the_downgrade_drops_execution_rows_but_keeps_the_ledger(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2: a populated execution table does not block a rollback.

    The ledger row an execution names outlives the downgrade; only the additive
    schema goes, and the re-upgrade then succeeds on the same database.
    """

    _require_revision()
    cfg = alembic_config()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    action_id = _insert_legacy_action(agent_id)
    _insert_execution(agent_id, subject_action_id=action_id)
    _insert_capability(agent_id)

    command.downgrade(cfg, BELOW)
    assert not _table_exists("action_executions")
    assert [row["id"] for row in _legacy_rows()] == [action_id]

    command.upgrade(cfg, REVISION)
    assert sql_rows("SELECT 1 FROM curie.action_executions") == []


@pytest.mark.parametrize("state", STATES)
def test_every_named_state_is_accepted(
    isolated_migration_db: IsolatedMigrationDb, state: str
) -> None:
    """@spec ACTION-EXECUTOR-2 @spec ACTION-EXECUTOR-17: the seven lifecycle states."""

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()

    _insert_execution(agent_id, kind="probe", tool=None, state=state)

    assert sql_rows("SELECT state FROM curie.action_executions")[0][0] == state


def test_an_unknown_state_violates_the_check(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec ACTION-EXECUTOR-2: "an unknown state violates the check"."""

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()

    with pytest.raises(IntegrityError):
        _insert_execution(agent_id, kind="probe", tool=None, state="retrying")


@pytest.mark.parametrize("kind", ["restore", "forward", "probe"])
def test_every_named_kind_is_accepted(
    isolated_migration_db: IsolatedMigrationDb, kind: str
) -> None:
    """@spec ACTION-EXECUTOR-2: ``kind`` is one of restore, forward, probe."""

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()

    _insert_execution(agent_id, kind=kind, tool=None if kind == "probe" else "restore")

    assert sql_rows("SELECT kind FROM curie.action_executions")[0][0] == kind


def test_an_unknown_kind_violates_the_check(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec ACTION-EXECUTOR-2: no fourth kind of execution can be stored."""

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()

    with pytest.raises(IntegrityError):
        _insert_execution(agent_id, kind="compensate")


def test_a_second_live_restore_of_one_action_violates_the_index(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2: "a second non-refused restore for one action violates the index"."""

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    action_id = _insert_legacy_action(agent_id)
    _insert_execution(agent_id, subject_action_id=action_id, state="requested")

    with pytest.raises(IntegrityError):
        _insert_execution(agent_id, subject_action_id=action_id, state="requested")


@pytest.mark.parametrize("first_state", LIVE_STATES)
def test_any_non_refused_restore_holds_the_action(
    isolated_migration_db: IsolatedMigrationDb, first_state: str
) -> None:
    """@spec ACTION-EXECUTOR-2 @spec ACTION-EXECUTOR-11

    Terminal states other than ``refused`` keep the action: a failed or
    indeterminate restore may have written, so it "still blocks a second undo",
    and a confirmed one already put the state back.
    """

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    action_id = _insert_legacy_action(agent_id)
    _insert_execution(agent_id, subject_action_id=action_id, state=first_state)

    with pytest.raises(IntegrityError):
        _insert_execution(agent_id, subject_action_id=action_id, state="requested")


def test_a_refused_restore_releases_the_action(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec ACTION-EXECUTOR-2 @spec ACTION-EXECUTOR-15

    ``refused`` is a provable non-write, so any number of refused restores may
    stand beside one later live restore of the same action.
    """

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    action_id = _insert_legacy_action(agent_id)
    _insert_execution(agent_id, subject_action_id=action_id, state="refused")
    _insert_execution(agent_id, subject_action_id=action_id, state="refused")

    _insert_execution(agent_id, subject_action_id=action_id, state="requested")

    assert len(sql_rows("SELECT 1 FROM curie.action_executions")) == 3


def test_live_restores_of_different_actions_do_not_collide(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2: the index is per action, not per agent or connector."""

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    first = _insert_legacy_action(agent_id, call_id="toolu_a")
    second = _insert_legacy_action(agent_id, call_id="toolu_b")

    _insert_execution(agent_id, subject_action_id=first, state="dispatched")
    _insert_execution(agent_id, subject_action_id=second, state="dispatched")

    assert len(sql_rows("SELECT 1 FROM curie.action_executions")) == 2


def test_the_index_covers_restores_only(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec ACTION-EXECUTOR-2: the partial index's predicate is ``kind = 'restore'``.

    A forward execution names the record it created at dispatch, so a live
    restore of that record must still be possible beside it.
    """

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    action_id = _insert_legacy_action(agent_id)
    _insert_execution(
        agent_id, kind="forward", tool="scale", subject_action_id=action_id, state="confirmed"
    )

    _insert_execution(agent_id, subject_action_id=action_id, state="requested")

    assert len(sql_rows("SELECT 1 FROM curie.action_executions")) == 2


def test_a_replayed_idempotency_key_cannot_create_a_second_row(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2: ``idempotency_key`` is unique within one agent.

    The constraint, not a writer-side check, is what lets a replayed creation
    adopt the existing row: two concurrent creators race a read, never a unique
    index. The conflict target is (``agent_id``, ``idempotency_key``), so the
    adoption a creator performs is scoped to its own agent. Exercised with
    probes so the restore index plays no part.
    """

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    key = f"probe:{agent_id}:k8s:{DIGEST}"
    _insert_execution(agent_id, kind="probe", tool=None, idempotency_key=key)

    with pytest.raises(IntegrityError):
        _insert_execution(agent_id, kind="probe", tool=None, idempotency_key=key)

    adopted = sql_dicts(
        "INSERT INTO curie.action_executions "
        "(id, kind, agent_id, connector, connector_digest, authority_kind, authority_ref, "
        "idempotency_key, state, attempt, created_at) "
        "VALUES (:id, 'probe', :agent_id, 'k8s', :digest, 'capability_probe', 'pass-2', "
        ":key, 'requested', 0, now()) "
        "ON CONFLICT (agent_id, idempotency_key) DO NOTHING RETURNING id",
        {"id": uuid.uuid4(), "agent_id": agent_id, "digest": DIGEST, "key": key},
    )
    assert adopted == []
    assert len(sql_rows("SELECT 1 FROM curie.action_executions")) == 1


def test_the_same_idempotency_key_under_another_agent_is_a_distinct_row(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2: "the same key under another agent creates a distinct row".

    One agent's key can never adopt another agent's execution: a forward
    authority owner supplies its own keys, and two agents may well choose the
    same one.
    """

    _require_revision()
    isolated_migration_db.at(REVISION)
    first = _insert_agent("restorer-a")
    second = _insert_agent("restorer-b")
    key = "forward:nightly-scale-down"

    _insert_execution(first, kind="probe", tool=None, idempotency_key=key)
    _insert_execution(second, kind="probe", tool=None, idempotency_key=key)

    rows = sql_dicts(
        "SELECT agent_id FROM curie.action_executions WHERE idempotency_key = :key",
        {"key": key},
    )
    assert sorted(str(row["agent_id"]) for row in rows) == sorted([str(first), str(second)])


def test_a_replay_under_another_agent_does_not_adopt_the_first_agents_row(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2: adoption on a replayed key stays inside one agent.

    The creator's conflict target is (``agent_id``, ``idempotency_key``): a
    second agent presenting the first agent's key inserts its own row instead of
    being handed the first agent's id.
    """

    _require_revision()
    isolated_migration_db.at(REVISION)
    first = _insert_agent("restorer-a")
    second = _insert_agent("restorer-b")
    key = "forward:nightly-scale-down"
    _insert_execution(first, kind="probe", tool=None, idempotency_key=key)

    created = sql_dicts(
        "INSERT INTO curie.action_executions "
        "(id, kind, agent_id, connector, connector_digest, authority_kind, authority_ref, "
        "idempotency_key, state, attempt, created_at) "
        "VALUES (:id, 'probe', :agent_id, 'k8s', :digest, 'capability_probe', 'pass-2', "
        ":key, 'requested', 0, now()) "
        "ON CONFLICT (agent_id, idempotency_key) DO NOTHING RETURNING id",
        {"id": uuid.uuid4(), "agent_id": second, "digest": DIGEST, "key": key},
    )

    assert len(created) == 1
    assert len(sql_rows("SELECT 1 FROM curie.action_executions")) == 2


def test_an_execution_under_another_agent_than_its_action_violates_the_foreign_key(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2: "an execution whose ``agent_id`` differs from its
    subject action's agent violates the foreign key".

    The composite key (``subject_action_id``, ``agent_id``) to the action's
    (``id``, ``agent_id``) means an execution always runs under the binding of
    the agent whose action it concerns. The same insert under the right agent
    succeeds, so the refusal is about the agent and nothing else.
    """

    _require_revision()
    isolated_migration_db.at(REVISION)
    owner = _insert_agent("restorer-a")
    other = _insert_agent("restorer-b")
    action_id = _insert_legacy_action(owner)

    with pytest.raises(IntegrityError):
        _insert_execution(other, subject_action_id=action_id, state="requested")

    _insert_execution(owner, subject_action_id=action_id, state="requested")
    assert len(sql_rows("SELECT 1 FROM curie.action_executions")) == 1


def test_a_forward_execution_under_another_agent_violates_the_foreign_key(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2: the composite key binds every kind that names an action."""

    _require_revision()
    isolated_migration_db.at(REVISION)
    owner = _insert_agent("restorer-a")
    other = _insert_agent("restorer-b")
    action_id = _insert_legacy_action(owner)

    with pytest.raises(IntegrityError):
        _insert_execution(
            other, kind="forward", tool="scale", subject_action_id=action_id, state="confirmed"
        )


def test_an_action_without_an_agent_cannot_be_the_subject_of_an_execution(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2 @spec ACTION-EXECUTOR-3: refused_no_agent, held by the schema.

    An action with no agent has no (``id``, ``agent_id``) pair any execution's
    agent can match, so no execution can ever run against it.
    """

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    orphan = _insert_legacy_action(None)

    with pytest.raises(IntegrityError):
        _insert_execution(agent_id, subject_action_id=orphan, state="requested")


def test_executions_die_with_their_agent(isolated_migration_db: IsolatedMigrationDb) -> None:
    """@spec ACTION-EXECUTOR-2: ``agent_id`` is a cascading, non-null foreign key."""

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    _insert_execution(agent_id, kind="probe", tool=None)

    sql_rows("DELETE FROM curie.agents WHERE id = :id", {"id": agent_id})

    assert sql_rows("SELECT 1 FROM curie.action_executions") == []


def test_an_execution_without_an_agent_is_refused(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-2: the call always runs under some agent's binding."""

    _require_revision()
    isolated_migration_db.at(REVISION)

    with pytest.raises(IntegrityError):
        sql_rows(
            "INSERT INTO curie.action_executions "
            "(id, kind, agent_id, connector, connector_digest, authority_kind, "
            "authority_ref, idempotency_key, state, attempt, created_at) "
            "VALUES (:id, 'probe', NULL, 'k8s', :digest, 'capability_probe', 'pass-1', "
            "'probe:none', 'requested', 0, now())",
            {"id": uuid.uuid4(), "digest": DIGEST},
        )


def test_one_capability_row_per_agent_connector_and_digest(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec ACTION-EXECUTOR-13: "the row is keyed on the digest".

    A second row for the same agent, connector and digest would leave
    ``undoable`` choosing between two answers; another digest is another image
    with its own tool list.
    """

    _require_revision()
    isolated_migration_db.at(REVISION)
    agent_id = _insert_agent()
    _insert_capability(agent_id)
    _insert_capability(agent_id, digest="sha256:" + "cd" * 32)
    _insert_capability(agent_id, connector="grafana")

    with pytest.raises(IntegrityError):
        _insert_capability(agent_id)

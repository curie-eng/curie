"""The twin memory expansion (ADR 0239): selfimp_sched_0001 -> selfimp_memory_0001.

The revision adds ``agents.agent_wide_learning`` (default false) and the
immutable ``memory_revisions`` table, and enriches every valid canonical fact
already stored (``memory`` / ``fact-<32 hex>`` whose value has a string
statement) to the typed shape: ``kind="fact"``, ``archived=false``, a title
derived from the statement's first line (at most 80 characters) and an unknown
``conversation``. Each enriched fact gets one ``baseline`` revision at its
current head version, so versions never reset. A malformed entry is left
exactly as it was: it stays inspectable and removable, never an instruction.

Runs against a database of its own (``isolated_migration_db``), never the
shared session database.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_dicts,
)
from alembic import command

BELOW = "selfimp_sched_0001"
REVISION = "selfimp_memory_0001"

VALID_FACT = "fact-" + "0123456789abcdef" * 2
CHANNEL_FACT = "fact-" + "11111111111111112222222222222222"
ANONYMOUS_FACT = "fact-" + "33333333333333334444444444444444"
MALFORMED_FACT = "fact-" + "fedcba9876543210" * 2
NON_OBJECT_FACT = "fact-" + "55555555555555556666666666666666"
BINDING = "slack:C0EXAMPLE1"

LONG_FIRST_LINE = "The deploy window is Tuesday afternoon " + "x" * 80
VALID_STATEMENT = LONG_FIRST_LINE + "\nand the freeze starts on Friday."


def _insert_entry(
    agent_id: uuid.UUID, scope: str | None, key: str, value: Any, version: int
) -> None:
    sql_dicts(
        "INSERT INTO curie.workflow_state_entries "
        "(id, agent_id, binding_scope, namespace, key, value, version) "
        "VALUES (:id, :agent, :scope, 'memory', :key, CAST(:value AS jsonb), :version)",
        {
            "id": uuid.uuid4(),
            "agent": agent_id,
            "scope": scope,
            "key": key,
            "value": json.dumps(value),
            "version": version,
        },
    )


def _entry(agent_id: uuid.UUID, scope: str | None, key: str) -> dict[str, Any]:
    rows = sql_dicts(
        "SELECT value::text AS value, version FROM curie.workflow_state_entries "
        "WHERE agent_id = :agent AND namespace = 'memory' AND key = :key "
        "AND binding_scope IS NOT DISTINCT FROM :scope",
        {"agent": agent_id, "key": key, "scope": scope},
    )
    assert len(rows) == 1, rows
    return {"value": json.loads(rows[0]["value"]), "version": rows[0]["version"]}


def _revisions(agent_id: uuid.UUID, key: str) -> list[dict[str, Any]]:
    rows = sql_dicts(
        "SELECT version, operation, actor, source, conversation, reverted_to_version, "
        "snapshot::text AS snapshot FROM curie.memory_revisions "
        "WHERE agent_id = :agent AND fact_id = :key ORDER BY version",
        {"agent": agent_id, "key": key},
    )
    for row in rows:
        row["snapshot"] = None if row["snapshot"] is None else json.loads(row["snapshot"])
    return rows


def _seed(agent_id: uuid.UUID) -> dict[str, Any]:
    valid = {
        "statement": VALID_STATEMENT,
        "author": "U0AUTHOR01",
        "stated_at": "2026-09-01T00:00:00Z",
        "session_id": "sess-old",
    }
    channel = {
        "statement": "Reply in threads.",
        "author": "U0AUTHOR02",
        "stated_at": "2026-09-02T00:00:00Z",
        "session_id": "sess-old-2",
    }
    anonymous = {"statement": "Sam approves vendor contracts."}
    malformed = {"statement": 42, "author": "U0AUTHOR03"}
    non_object = "a bare string someone stored by hand"
    _insert_entry(agent_id, None, VALID_FACT, valid, 3)
    _insert_entry(agent_id, BINDING, CHANNEL_FACT, channel, 1)
    _insert_entry(agent_id, None, ANONYMOUS_FACT, anonymous, 2)
    _insert_entry(agent_id, None, MALFORMED_FACT, malformed, 5)
    _insert_entry(agent_id, None, NON_OBJECT_FACT, non_object, 1)
    _insert_entry(agent_id, None, "guidance", {"text": "operator guidance"}, 1)
    _insert_entry(agent_id, None, "log", [{"content": "legacy", "provenance": {}}], 4)
    return {
        "valid": valid,
        "channel": channel,
        "anonymous": anonymous,
        "malformed": malformed,
        "non_object": non_object,
    }


def _upgrade(isolated_migration_db: IsolatedMigrationDb) -> tuple[uuid.UUID, dict[str, Any]]:
    isolated_migration_db.at(BELOW)
    agent_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name) VALUES (:id, 'twin-memory-migration')",
        {"id": agent_id},
    )
    seeded = _seed(agent_id)
    command.upgrade(alembic_config(), REVISION)
    return agent_id, seeded


def test_the_revision_is_one_expand_on_the_agent_schedules_head() -> None:
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(alembic_config())
    revision = script.get_revision(REVISION)
    assert revision is not None
    assert revision.down_revision == BELOW
    assert script.get_heads() == [REVISION]


def test_upgrade_adds_a_default_off_learning_toggle(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    agent_id, _ = _upgrade(isolated_migration_db)
    assert "agent_wide_learning" in column_names("agents")
    assert sql_dicts(
        "SELECT agent_wide_learning FROM curie.agents WHERE id = :id", {"id": agent_id}
    ) == [{"agent_wide_learning": False}]
    # A row inserted after the upgrade without naming the column is off too
    # (server default), and the column refuses null.
    fresh = uuid.uuid4()
    sql_dicts("INSERT INTO curie.agents (id, name) VALUES (:id, 'after-upgrade')", {"id": fresh})
    assert sql_dicts(
        "SELECT agent_wide_learning FROM curie.agents WHERE id = :id", {"id": fresh}
    ) == [{"agent_wide_learning": False}]
    nullable = sql_dicts(
        "SELECT is_nullable FROM information_schema.columns WHERE table_schema = 'curie' "
        "AND table_name = 'agents' AND column_name = 'agent_wide_learning'"
    )
    assert nullable == [{"is_nullable": "NO"}]


def test_upgrade_creates_the_revision_table(isolated_migration_db: IsolatedMigrationDb) -> None:
    _upgrade(isolated_migration_db)
    assert {
        "id",
        "agent_id",
        "scope",
        "fact_id",
        "version",
        "operation",
        "snapshot",
        "actor",
        "source",
        "conversation",
        "reverted_to_version",
        "created_at",
    } <= column_names("memory_revisions")


def test_valid_facts_are_enriched_with_a_baseline_revision_at_their_version(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    agent_id, seeded = _upgrade(isolated_migration_db)

    head = _entry(agent_id, None, VALID_FACT)
    # The version is kept, not reset.
    assert head["version"] == 3
    expected = {
        **seeded["valid"],
        "kind": "fact",
        "archived": False,
        "title": LONG_FIRST_LINE[:80],
        "conversation": None,
    }
    assert head["value"] == expected
    assert len(head["value"]["title"]) == 80
    assert "\n" not in head["value"]["title"]

    revisions = _revisions(agent_id, VALID_FACT)
    assert len(revisions) == 1, revisions
    baseline = revisions[0]
    assert baseline["version"] == 3
    assert baseline["operation"] == "baseline"
    assert baseline["source"] == "migration"
    # Stored provenance is preserved, never invented.
    assert baseline["actor"] == "U0AUTHOR01"
    assert baseline["conversation"] is None
    assert baseline["reverted_to_version"] is None
    assert baseline["snapshot"] == expected


def test_binding_scoped_and_authorless_facts_are_enriched_too(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    agent_id, seeded = _upgrade(isolated_migration_db)

    channel = _entry(agent_id, BINDING, CHANNEL_FACT)
    assert channel["version"] == 1
    assert channel["value"] == {
        **seeded["channel"],
        "kind": "fact",
        "archived": False,
        "title": "Reply in threads.",
        "conversation": None,
    }
    [channel_baseline] = _revisions(agent_id, CHANNEL_FACT)
    assert (channel_baseline["version"], channel_baseline["operation"]) == (1, "baseline")
    assert channel_baseline["actor"] == "U0AUTHOR02"

    anonymous = _entry(agent_id, None, ANONYMOUS_FACT)
    assert anonymous["version"] == 2
    assert anonymous["value"]["kind"] == "fact"
    assert anonymous["value"]["title"] == "Sam approves vendor contracts."
    assert anonymous["value"]["conversation"] is None
    # No author is invented in the value; the revision says it is unknown.
    assert "author" not in anonymous["value"]
    [anonymous_baseline] = _revisions(agent_id, ANONYMOUS_FACT)
    assert anonymous_baseline["version"] == 2
    assert anonymous_baseline["actor"] == "<unknown>"
    assert anonymous_baseline["source"] == "migration"


def test_malformed_and_non_fact_entries_are_untouched(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    agent_id, seeded = _upgrade(isolated_migration_db)

    malformed = _entry(agent_id, None, MALFORMED_FACT)
    assert malformed == {"value": seeded["malformed"], "version": 5}
    assert _revisions(agent_id, MALFORMED_FACT) == []

    non_object = _entry(agent_id, None, NON_OBJECT_FACT)
    assert non_object == {"value": seeded["non_object"], "version": 1}
    assert _revisions(agent_id, NON_OBJECT_FACT) == []

    assert _entry(agent_id, None, "guidance") == {
        "value": {"text": "operator guidance"},
        "version": 1,
    }
    assert _entry(agent_id, None, "log")["version"] == 4
    assert _revisions(agent_id, "guidance") == []
    assert _revisions(agent_id, "log") == []
    # Exactly the three valid facts received a baseline.
    total = sql_dicts(
        "SELECT count(*) AS n FROM curie.memory_revisions WHERE agent_id = :agent",
        {"agent": agent_id},
    )
    assert total == [{"n": 3}]


def test_deleting_the_agent_removes_its_revisions(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    agent_id, _ = _upgrade(isolated_migration_db)
    assert _revisions(agent_id, VALID_FACT)
    sql_dicts("DELETE FROM curie.agents WHERE id = :id", {"id": agent_id})
    assert sql_dicts(
        "SELECT count(*) AS n FROM curie.memory_revisions WHERE agent_id = :agent",
        {"agent": agent_id},
    ) == [{"n": 0}]

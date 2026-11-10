"""Real migration evidence. @spec AGENT-ROLE-PERSISTENCE-V1-1/2/9."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    constraint_exists,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from alembic.script import ScriptDirectory
from role_policy_support import role
from sqlalchemy.exc import IntegrityError

ROOT = Path(__file__).resolve().parents[3]


def revision_ready() -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-9. Absent migration is a body assertion, not setup error."""
    graph = ScriptDirectory.from_config(alembic_config())
    assert "0133" in {r.revision for r in graph.walk_revisions()}, (
        "role storage migration 0133 is missing"
    )
    assert graph.get_revision("0133").down_revision == "0132"


def old_policy() -> dict[str, uuid.UUID]:
    """@spec AGENT-ROLE-PERSISTENCE-V1-1/9. Seed only columns that actually exist in 0132."""
    ids = {name: uuid.uuid4() for name in ("tenant", "agent", "invocation", "approver")}
    sql_rows(
        "INSERT INTO curie.tenants (id,deployment_id,status) VALUES (:tenant,:name,'active')",
        {**ids, "name": f"synthetic-role-upgrade-{ids['tenant']}"},
    )
    sql_rows(
        "INSERT INTO curie.agents (id,tenant_id,name) VALUES (:agent,:tenant,:name)",
        {**ids, "name": f"synthetic-role-upgrade-{ids['agent']}"},
    )
    for target in ("invocation", "approver"):
        sql_rows(
            "INSERT INTO curie.teams (id,tenant_id,source,external_id,name) "
            "VALUES (:id,:tenant,'idp_group',:external,:name)",
            {
                "id": ids[target],
                "tenant": ids["tenant"],
                "external": target,
                "name": f"synthetic-role-{target}-{ids[target]}",
            },
        )
    sql_rows(
        "INSERT INTO curie.agent_access_policies "
        "(agent_id,tenant_id,invocation_team_id,approver_team_id,version,updated_at) "
        "VALUES (:agent,:tenant,:invocation,:approver,7,'2029-01-02T03:04:05Z')",
        ids,
    )
    return ids


def test_upgrade_preserves_legacy_rows_and_real_object_check(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-1/2/9. Migration must not backfill or alter old policy."""
    isolated_migration_db.at("0132")
    revision_ready()
    ids = old_policy()
    before = sql_dicts("SELECT * FROM curie.agent_access_policies WHERE agent_id=:agent", ids)[0]
    command.upgrade(alembic_config(), "0133")
    assert stamped_revision() == "0133"
    after = sql_dicts("SELECT * FROM curie.agent_access_policies WHERE agent_id=:agent", ids)[0]
    assert after == {**before, "role_configuration": None}
    assert sql_dicts(
        "SELECT role_configuration IS NULL AS sql_null,"
        "jsonb_typeof(role_configuration) AS json_type FROM curie.agent_access_policies "
        "WHERE agent_id=:agent",
        ids,
    ) == [{"sql_null": True, "json_type": None}]
    assert constraint_exists("agent_access_policies_role_object_ck")
    for literal in ("null", "[]", "3", '"text"', "true"):
        with pytest.raises(IntegrityError) as error:
            sql_rows(
                "UPDATE curie.agent_access_policies SET role_configuration=CAST(:value AS jsonb) "
                "WHERE agent_id=:agent",
                {**ids, "value": literal},
            )
        cause = error.value.orig.__cause__
        assert getattr(cause, "constraint_name", None) == "agent_access_policies_role_object_ck"
    sql_rows(
        "UPDATE curie.agent_access_policies SET role_configuration='{}'::jsonb "
        "WHERE agent_id=:agent",
        ids,
    )
    assert sql_dicts(
        "SELECT role_configuration,role_configuration IS NULL AS sql_null "
        "FROM curie.agent_access_policies WHERE agent_id=:agent",
        ids,
    ) == [{"role_configuration": {}, "sql_null": False}]


@pytest.mark.parametrize(
    "value",
    [None, {}, {"invocation": role()}, {"approver": role(migrated=True)}, "corrupt_json_null"],
)
def test_downgrade_requires_every_new_value_to_be_sql_null(
    isolated_migration_db: IsolatedMigrationDb, value: object
) -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-9. Empty object is data and may not be silently discarded."""
    isolated_migration_db.at("0132")
    revision_ready()
    ids = old_policy()
    command.upgrade(alembic_config(), "0133")
    if value == "corrupt_json_null":
        sql_rows(
            "ALTER TABLE curie.agent_access_policies "
            "DROP CONSTRAINT agent_access_policies_role_object_ck"
        )
    if value is not None:
        sql_rows(
            "UPDATE curie.agent_access_policies SET role_configuration=CAST(:value AS jsonb) "
            "WHERE agent_id=:agent",
            {**ids, "value": "null" if value == "corrupt_json_null" else json.dumps(value)},
        )
        with pytest.raises(RuntimeError, match="^agent_role_configuration_present$"):
            command.downgrade(alembic_config(), "0132")
        assert stamped_revision() == "0133"
        assert "role_configuration" in column_names("agent_access_policies")
    else:
        command.downgrade(alembic_config(), "0132")
        assert stamped_revision() == "0132"
        assert "role_configuration" not in column_names("agent_access_policies")
        assert sql_dicts(
            "SELECT version,invocation_team_id,approver_team_id "
            "FROM curie.agent_access_policies WHERE agent_id=:agent",
            ids,
        ) == [
            {
                "version": 7,
                "invocation_team_id": ids["invocation"],
                "approver_team_id": ids["approver"],
            }
        ]


def test_candidate_floor_and_graph_follow_actual_role_schema_without_releasing() -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-9. A candidate reading the new column cannot serve0132."""
    revision_ready()
    catalog = json.loads((ROOT / "cli/src/application_schema_windows.json").read_text())
    revisions = catalog["revisions"]
    # Later candidates may raise the floor further; it never drops below 0133.
    expected = catalog["candidate"]
    assert revisions.index(expected["schema_min"]) >= revisions.index("0133")
    assert json.loads((ROOT / "apps/api/src/curie_api/schema_compat.json").read_text()) == expected
    shared = json.loads(
        (
            ROOT / "packages/protected-hooks/src/curie_protected_hooks/schema_serving.json"
        ).read_text()
    )
    assert {name: shared[name] for name in expected} == expected
    assert shared["revision_parents"]["0133"] == ["0132"]
    assert (
        json.loads((ROOT / "apps/api/src/curie_api/revision_kinds.json").read_text())["0133"]
        == "expand"
    )
    assert revisions[revisions.index("0133") - 1] == "0132"
    assert catalog["windows"]["0.14.1"] == {"schema_min": "0123", "schema_head": "0123"}

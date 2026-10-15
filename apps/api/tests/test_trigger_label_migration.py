"""The trigger label migration (#4361 AC 2, decisions 3, 4 and 19, as amended by
#4397 decision 10).

A database stamped by the latest release is upgraded to head. The migration
adds ``agent_channels.trigger_label`` with a CHECK that admits it on any tracker
surface kind (``github``, ``gitlab``, ``jira``) and refuses it elsewhere, widens
``agent_channels_route_key`` to ``(kind, address, adapter, trigger_label)`` with
NULLS NOT DISTINCT, and adds ``work_items.trigger_label`` (the label that
admitted a WorkItem; the cancellation path reads it). Downgrade refuses while a
location holds two bindings and otherwise drops both columns, the CHECK and the
widened key.

Every test runs on a private database (``isolated_migration_db``), never the
shared one, per apps/api/CLAUDE.md.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
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
from sqlalchemy.exc import DBAPIError, IntegrityError

# The schema head of the latest release (v0.12.3); check-released-upgrade.py
# derives the same tag from git and upgrades a real stamped database.
RELEASED_HEAD = "0093"
REPO = "acme-corp/acme-bot"
FAST = "curie-factory-fast"
ROUTE_KEY = "agent_channels_route_key"
LABEL_CHECK = "agent_channels_trigger_label_ck"


def _label_revision() -> tuple[str, str]:
    """The migration that adds the trigger label, and the revision it sits on.

    Found by its file name (plan: ``<REV>_agent_channel_trigger_label.py``) so
    the test does not hard-code a number the train may still move.
    """

    script = ScriptDirectory.from_config(alembic_config())
    found = [rev for rev in script.walk_revisions() if "trigger_label" in Path(rev.path).name]
    assert len(found) == 1, (
        "exactly one migration named *trigger_label* must add agent_channels.trigger_label; "
        f"found {[rev.revision for rev in found]}"
    )
    (revision,) = found
    down = revision.down_revision
    assert isinstance(down, str), down
    return revision.revision, down


def _constraint_definition(name: str) -> str:
    rows = sql_rows(
        "SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c "
        "JOIN pg_class t ON t.oid = c.conrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "WHERE n.nspname = 'curie' AND c.conname = :name",
        {"name": name},
    )
    assert len(rows) == 1, (name, rows)
    return str(rows[0][0])


def _agent() -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name, repo_full_name) VALUES (:id, :name, :repo)",
        {"id": agent_id, "name": f"acme-factory-{agent_id.hex[:8]}", "repo": REPO},
    )
    return agent_id


def _bind(
    agent_id: uuid.UUID,
    kind: str,
    address: str,
    *,
    trigger_label: str | None = None,
    adapter: str | None = None,
) -> uuid.UUID:
    binding_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address, adapter, trigger_label) "
        "VALUES (:id, :agent, :kind, :address, :adapter, :label)",
        {
            "id": binding_id,
            "agent": agent_id,
            "kind": kind,
            "address": address,
            "adapter": adapter,
            "label": trigger_label,
        },
    )
    return binding_id


def _work_item(agent_id: uuid.UUID, number: int, trigger_label: str | None) -> uuid.UUID:
    work_item_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.work_items (id, tracker_kind, tracker_host, tracker_scope_id, "
        "tracker_issue_id, code_host_kind, code_host_host, repository_project_id, "
        "repository_path, code_host_installation_id, agent_id, conversation_id, trigger_label) "
        "VALUES (:id, 'github', 'github.com', '4401', :number, 'github', 'github.com', "
        "'4401', :repo, 5501, :agent, :conversation, :label)",
        {
            "id": work_item_id,
            "number": str(number),
            "repo": REPO,
            "agent": agent_id,
            "conversation": f"github:{REPO}:issue-{number}",
            "label": trigger_label,
        },
    )
    return work_item_id


@pytest.fixture
def upgraded(isolated_migration_db: IsolatedMigrationDb) -> Iterator[tuple[str, str]]:
    """A private database stamped by the latest release, then upgraded to head."""

    revision, down = _label_revision()
    isolated_migration_db.at(RELEASED_HEAD)
    assert stamped_revision() == RELEASED_HEAD
    assert "trigger_label" not in column_names("agent_channels")
    command.upgrade(alembic_config(), "head")
    yield revision, down


def test_trigger_label_migration_upgrades_a_released_database_and_adds_both_columns(
    upgraded: tuple[str, str],
) -> None:
    assert "trigger_label" in column_names("agent_channels")
    assert "trigger_label" in column_names("work_items")
    assert constraint_exists(LABEL_CHECK)
    key = _constraint_definition(ROUTE_KEY)
    assert "NULLS NOT DISTINCT" in key, key
    assert "(kind, address, adapter, trigger_label)" in key, key


@pytest.mark.parametrize(
    ("kind", "address"),
    [("github", REPO), ("gitlab", "acme/platform/infra"), ("jira", "acme-jira")],
)
def test_trigger_label_migration_check_admits_a_label_on_each_tracker_kind(
    upgraded: tuple[str, str], kind: str, address: str
) -> None:
    """#4397 decision 10: the label may sit on any tracker surface kind."""

    binding_id = _bind(_agent(), kind, address, trigger_label=FAST)

    assert sql_dicts(
        "SELECT kind, trigger_label FROM curie.agent_channels WHERE id = :id",
        {"id": binding_id},
    ) == [{"kind": kind, "trigger_label": FAST}]


def test_trigger_label_migration_check_refuses_a_label_on_slack(
    upgraded: tuple[str, str],
) -> None:
    agent_id = _agent()

    with pytest.raises(IntegrityError, match=LABEL_CHECK):
        _bind(agent_id, "slack", "C0EXAMPLE1", trigger_label=FAST, adapter="default")

    # Liveness: the same Slack row without a label is accepted.
    _bind(agent_id, "slack", "C0EXAMPLE1", adapter="default")


def test_trigger_label_migration_key_admits_two_labels_and_refuses_two_nulls(
    upgraded: tuple[str, str],
) -> None:
    """The widened key keeps NULLS NOT DISTINCT: two NULL-label rows on one
    route still collide, while a NULL row and a labelled row coexist."""

    _bind(_agent(), "github", REPO)
    _bind(_agent(), "github", REPO, trigger_label=FAST)

    with pytest.raises(IntegrityError, match=ROUTE_KEY):
        _bind(_agent(), "github", REPO)
    with pytest.raises(IntegrityError, match=ROUTE_KEY):
        _bind(_agent(), "github", REPO, trigger_label=FAST)
    assert (
        len(sql_dicts("SELECT id FROM curie.agent_channels WHERE address = :a", {"a": REPO})) == 2
    )


def test_trigger_label_migration_downgrade_refuses_while_a_location_holds_two_bindings(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    revision, down = _label_revision()
    # Start exactly at this migration. Downgrading a later merge head can
    # legitimately unwind other revisions before reaching this refusal.
    isolated_migration_db.at(revision)
    assert stamped_revision() == revision
    _bind(_agent(), "github", REPO)
    labelled = _bind(_agent(), "github", REPO, trigger_label=FAST)

    # The migration raises its own error naming the location; the type is its choice.
    with pytest.raises(Exception) as refused:  # noqa: B017, PT011
        command.downgrade(alembic_config(), down)

    assert REPO in str(refused.value)
    # Refused whole: nothing was dropped and the stamp did not move.
    assert stamped_revision() == revision
    assert "trigger_label" in column_names("agent_channels")
    assert "trigger_label" in column_names("work_items")
    assert constraint_exists(LABEL_CHECK)

    # Liveness: once one binding is removed the same downgrade succeeds.
    sql_dicts("DELETE FROM curie.agent_channels WHERE id = :id", {"id": labelled})
    command.downgrade(alembic_config(), down)

    assert stamped_revision() == down


def test_trigger_label_migration_downgrade_drops_both_columns_and_restores_the_key(
    upgraded: tuple[str, str],
) -> None:
    _revision, down = upgraded
    agent_id = _agent()
    binding_id = _bind(agent_id, "github", REPO, trigger_label=FAST)
    _bind(agent_id, "slack", "C0EXAMPLE1", adapter="default")

    command.downgrade(alembic_config(), down)

    assert "trigger_label" not in column_names("agent_channels")
    assert "trigger_label" not in column_names("work_items")
    assert not constraint_exists(LABEL_CHECK)
    key = _constraint_definition(ROUTE_KEY)
    assert "trigger_label" not in key, key
    assert "(kind, address, adapter)" in key, key
    assert "NULLS NOT DISTINCT" in key, key
    # The binding itself survives the downgrade, without its label.
    assert sql_dicts(
        "SELECT kind, address FROM curie.agent_channels WHERE id = :id", {"id": binding_id}
    ) == [{"kind": "github", "address": REPO}]
    # And upgrading again is clean (the downgrade left no half-state behind).
    command.upgrade(alembic_config(), "head")
    assert "trigger_label" in column_names("agent_channels")


def test_trigger_label_migration_downgrade_keeps_a_lone_labelled_surface_as_the_route(
    upgraded: tuple[str, str],
) -> None:
    """A lone explicitly labelled surface survives downgrade with its label
    discarded, so it alone holds the route: the predecessor sends that
    location's install-label issues to its agent."""

    _revision, down = upgraded
    fast_agent = _agent()
    binding_id = _bind(fast_agent, "github", REPO, trigger_label=FAST)

    command.downgrade(alembic_config(), down)

    assert sql_dicts(
        "SELECT id, agent_id FROM curie.agent_channels WHERE kind = 'github' AND address = :a",
        {"a": REPO},
    ) == [{"id": binding_id, "agent_id": fast_agent}]
    # The route is now held unlabelled: another agent cannot bind it.
    with pytest.raises(IntegrityError, match=ROUTE_KEY):
        sql_dicts(
            "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
            "VALUES (:id, :agent, 'github', :address)",
            {"id": uuid.uuid4(), "agent": _agent(), "address": REPO},
        )


def test_trigger_label_migration_work_item_label_persists_and_rolls_back(
    upgraded: tuple[str, str],
) -> None:
    """``work_items.trigger_label`` stores the admitting label, NULL for a row
    admitted before the migration; a failed insert leaves no row and no label;
    downgrade removes the column with the WorkItem kept."""

    _revision, down = upgraded
    agent_id = _agent()
    labelled = _work_item(agent_id, 12, FAST)
    unlabelled = _work_item(agent_id, 13, None)

    rows = {
        row["id"]: row["trigger_label"]
        for row in sql_dicts("SELECT id, trigger_label FROM curie.work_items")
    }
    assert rows == {labelled: FAST, unlabelled: None}

    # The column is bounded like the binding's label.
    with pytest.raises(DBAPIError):
        _work_item(agent_id, 14, "x" * 51)
    # A failed admission transaction (the issue's identity is already taken)
    # leaves neither a row nor its label behind.
    with pytest.raises(IntegrityError):
        _work_item(agent_id, 12, "curie-factory")
    assert sql_dicts(
        "SELECT trigger_label FROM curie.work_items WHERE tracker_issue_id = '12'"
    ) == [{"trigger_label": FAST}]
    assert len(sql_dicts("SELECT id FROM curie.work_items")) == 2

    command.downgrade(alembic_config(), down)

    assert "trigger_label" not in column_names("work_items")
    assert {row["id"] for row in sql_dicts("SELECT id FROM curie.work_items")} == {
        labelled,
        unlabelled,
    }

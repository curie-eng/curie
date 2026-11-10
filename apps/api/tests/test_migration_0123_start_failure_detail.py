"""Migration 0123 records an execution request's previous start failure (#4930).

Expand-only: one nullable ``last_start_failure_detail`` text column on
``curie.execution_requests`` and a check that a stored value is nonblank and at
most 2000 characters. Nothing is backfilled, so a request written at 0122
reads NULL. The downgrade drops the constraint and the column.

0123 rather than a suffixed id: ``main`` and ``next`` both stop at 0122, and the
number right after a shared head is the one a stable fix takes next.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    constraint_exists,
    sql_dicts,
)
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy.exc import IntegrityError

COLUMN = "last_start_failure_detail"
CONSTRAINT = "execution_requests_last_start_failure_detail_ck"
REPO = "acme-corp/acme-bot"
OBJECTIVE = "Implement the admitted work item"
REQUESTER = "U0REQUEST1"
DETAIL = "workspace archive-size failed: archive exceeds 1048576 bytes"


def _seed_waiting_request() -> uuid.UUID:
    """An agent, its WorkItem and one waiting request, in the 0122 shape.

    The request columns follow the insert of
    test_migration_0047_work_item_dispatch.py / test_migration_0103_tracker_issue_identity.py;
    the WorkItem uses the tracker-keyed identity 0103 introduced.
    """

    agent_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.agents (id, name, repo_full_name) VALUES (:id, :name, :repo)",
        {"id": agent_id, "name": f"start-detail-{agent_id.hex[:8]}", "repo": REPO},
    )
    work_item_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.work_items (id, tracker_kind, tracker_host, tracker_scope_id, "
        "tracker_issue_id, code_host_kind, code_host_host, repository_project_id, "
        "repository_path, code_host_installation_id, agent_id, conversation_id) "
        "VALUES (:id, 'github', 'github.com', '4930', '4930', 'github', 'github.com', "
        "'4930', :repo, 5501, :agent, :conversation)",
        {
            "id": work_item_id,
            "repo": REPO,
            "agent": agent_id,
            "conversation": f"github:{REPO}:issue-4930",
        },
    )
    request_id = uuid.uuid4()
    sql_dicts(
        "INSERT INTO curie.execution_requests "
        "(id, work_item_id, sequence, status, wait_deadline, capacity_deferrals, "
        "last_deferral_reason, objective, requester, "
        "reply_kind, reply_address, reply_conversation_id) VALUES (:id, :item, 1, 'waiting', "
        "clock_timestamp() + interval '30 minutes', 2, 'capacity', :objective, :requester, "
        "'slack', 'C0EXAMPLE1', '1700000000.000100')",
        {"id": request_id, "item": work_item_id, "objective": OBJECTIVE, "requester": REQUESTER},
    )
    return request_id


def _request(request_id: uuid.UUID) -> dict[str, Any]:
    rows = sql_dicts("SELECT * FROM curie.execution_requests WHERE id = :id", {"id": request_id})
    assert len(rows) == 1
    return rows[0]


def _set_detail(request_id: uuid.UUID, detail: str | None) -> None:
    sql_dicts(
        f"UPDATE curie.execution_requests SET {COLUMN} = :detail WHERE id = :id",
        {"id": request_id, "detail": detail},
    )


@pytest.fixture
def at_0123(isolated_migration_db: IsolatedMigrationDb) -> Iterator[None]:
    isolated_migration_db.at("0123")
    yield


def test_0123_revises_0122() -> None:
    script = ScriptDirectory.from_config(alembic_config())
    revision = script.get_revision("0123")
    assert revision is not None
    assert revision.down_revision == "0122"
    assert revision.path.endswith("0123_execution_request_start_failure_detail.py")


def test_0123_round_trip_adds_and_drops_the_column_and_constraint(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at("0123")
    assert COLUMN in column_names("execution_requests")
    assert constraint_exists(CONSTRAINT)
    nullable = sql_dicts(
        "SELECT is_nullable, data_type FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'execution_requests' "
        "AND column_name = :column",
        {"column": COLUMN},
    )
    assert nullable == [{"is_nullable": "YES", "data_type": "text"}]
    try:
        command.downgrade(config, "0122")
        assert COLUMN not in column_names("execution_requests")
        assert not constraint_exists(CONSTRAINT)
        command.upgrade(config, "0123")
        assert COLUMN in column_names("execution_requests")
        assert constraint_exists(CONSTRAINT)
    finally:
        command.upgrade(config, "head")


def test_0123_existing_requests_read_null(isolated_migration_db: IsolatedMigrationDb) -> None:
    isolated_migration_db.at("0122")
    request_id = _seed_waiting_request()
    before = _request(request_id)
    assert COLUMN not in before

    command.upgrade(alembic_config(), "0123")

    after = _request(request_id)
    assert after[COLUMN] is None
    # Nothing else on the row moves: no backfill, no version bump.
    assert {key: value for key, value in after.items() if key != COLUMN} == before


@pytest.mark.parametrize(
    "refused",
    ["", " ", "  \n\t ", "x" * 2001],
    ids=["empty", "space", "whitespace", "over_2000"],
)
def test_0123_constraint_refuses_blank_and_overlong_detail(at_0123: None, refused: str) -> None:
    request_id = _seed_waiting_request()
    with pytest.raises(IntegrityError, match=CONSTRAINT):
        _set_detail(request_id, refused)
    assert _request(request_id)[COLUMN] is None

    # Liveness: a real detail, one exactly at the bound, and NULL are accepted.
    _set_detail(request_id, DETAIL)
    assert _request(request_id)[COLUMN] == DETAIL
    _set_detail(request_id, "x" * 2000)
    assert _request(request_id)[COLUMN] == "x" * 2000
    _set_detail(request_id, None)
    assert _request(request_id)[COLUMN] is None

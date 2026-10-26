"""Raise the execution deadline ceiling from 10800 s to 43200 s (#5067).

Widens ``agents_execution_deadline_seconds_ck`` to 60..43200 and
``execution_requests_deadline_ck`` to at most 43200 s after ``started_at``.
No data changes. Downgrade restores the 10800 s checks and refuses, changing
nothing, while any agent or execution request carries a longer deadline.

Revision ID: 0136
Revises: 0131
Create Date: 2026-10-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0136"
down_revision: str | None = "0131"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
DEADLINE_CK = "execution_requests_deadline_ck"
AGENT_CK = "agents_execution_deadline_seconds_ck"
OLD_MAX = 10800
NEW_MAX = 43200


def _agent_text(maximum: int) -> str:
    return (
        f"execution_deadline_seconds IS NULL OR execution_deadline_seconds BETWEEN 60 AND {maximum}"
    )


def _request_text(maximum: int) -> str:
    return (
        "(started_at IS NULL AND execution_deadline IS NULL) OR "
        "(started_at IS NOT NULL AND execution_deadline IS NOT NULL AND "
        "execution_deadline > started_at AND "
        f"execution_deadline <= started_at + interval '{maximum} seconds')"
    )


def _replace(maximum: int) -> None:
    op.drop_constraint(AGENT_CK, "agents", schema=SCHEMA, type_="check")
    op.create_check_constraint(AGENT_CK, "agents", _agent_text(maximum), schema=SCHEMA)
    op.drop_constraint(DEADLINE_CK, "execution_requests", schema=SCHEMA, type_="check")
    op.create_check_constraint(
        DEADLINE_CK, "execution_requests", _request_text(maximum), schema=SCHEMA
    )


def upgrade() -> None:
    _replace(NEW_MAX)


def downgrade() -> None:
    bind = op.get_bind()
    agents = bind.execute(
        sa.text(f"SELECT count(*) FROM curie.agents WHERE execution_deadline_seconds > {OLD_MAX}")
    ).scalar_one()
    requests = bind.execute(
        sa.text(
            "SELECT count(*) FROM curie.execution_requests "
            f"WHERE execution_deadline > started_at + interval '{OLD_MAX} seconds'"
        )
    ).scalar_one()
    if agents or requests:
        raise RuntimeError(
            f"cannot downgrade below 0136: {int(agents)} agents have "
            f"execution_deadline_seconds above {OLD_MAX} and {int(requests)} "
            f"execution requests have a deadline more than {OLD_MAX} seconds "
            "after started_at; clear those values first"
        )
    _replace(OLD_MAX)

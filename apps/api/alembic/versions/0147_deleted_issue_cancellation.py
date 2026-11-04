"""Allow deleted tracker issues to cancel execution requests.

Revision ID: 0147
Revises: 0145
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0147"
down_revision: str = "0145"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OLD_SHAPE = (
    "((status = 'failed' AND started_at IS NULL AND execution_deadline IS NULL AND terminal_at "
    "IS NOT NULL AND terminal_cause = 'start_failed' AND termination_observation IS NULL) OR "
    "(status = 'queued' AND wait_deadline IS NULL AND started_at IS NULL AND execution_deadline"
    " IS NULL AND terminal_at IS NULL AND terminal_cause IS NULL AND termination_observation IS"
    " NULL) OR (status = 'waiting' AND wait_deadline IS NOT NULL AND started_at IS NULL AND "
    "execution_deadline IS NULL AND terminal_at IS NULL AND terminal_cause IS NULL AND "
    "termination_observation IS NULL) OR (status = 'running' AND started_at IS NOT NULL AND "
    "execution_deadline IS NOT NULL AND terminal_at IS NULL AND terminal_cause IS NULL AND "
    "termination_observation IS NULL) OR (status = 'cancellation_requested' AND started_at IS "
    "NOT NULL AND execution_deadline IS NOT NULL AND terminal_at IS NULL AND terminal_cause IS "
    "NOT NULL AND terminal_cause IN ('issue_cancelled', 'execution_deadline', 'owner_lost') AND"
    " termination_observation IS NULL) OR (status = 'completed' AND started_at IS NOT NULL AND "
    "execution_deadline IS NOT NULL AND terminal_at IS NOT NULL AND terminal_cause IS NOT NULL "
    "AND terminal_cause = 'completed' AND termination_observation IS NULL) OR (status = "
    "'failed' AND started_at IS NOT NULL AND execution_deadline IS NOT NULL AND terminal_at IS "
    "NOT NULL AND terminal_cause IS NOT NULL AND ((terminal_cause = 'owner_lost' AND "
    "termination_observation IS NOT NULL) OR (terminal_cause <> 'owner_lost' AND "
    "termination_observation IS NULL))) OR (status = 'expired' AND terminal_at IS NOT NULL AND "
    "((started_at IS NULL AND execution_deadline IS NULL AND terminal_cause IS NOT NULL AND "
    "terminal_cause = 'capacity_wait_expired' AND termination_observation IS NULL) OR "
    "(started_at IS NOT NULL AND execution_deadline IS NOT NULL AND terminal_cause IS NOT NULL "
    "AND terminal_cause = 'execution_deadline' AND termination_observation IS NOT NULL))) OR "
    "(status = 'cancelled' AND terminal_at IS NOT NULL AND terminal_cause IS NOT NULL AND "
    "terminal_cause IN ('issue_cancelled', 'lineage_closed') AND ((started_at IS NULL AND "
    "execution_deadline IS NULL AND termination_observation IS NULL) OR (started_at IS NOT NULL"
    " AND execution_deadline IS NOT NULL AND termination_observation IS NOT NULL)))) IS TRUE"
)
_NEW_SHAPE = (
    "((status = 'failed' AND started_at IS NULL AND execution_deadline IS NULL AND terminal_at "
    "IS NOT NULL AND terminal_cause = 'start_failed' AND termination_observation IS NULL) OR "
    "(status = 'queued' AND wait_deadline IS NULL AND started_at IS NULL AND execution_deadline"
    " IS NULL AND terminal_at IS NULL AND terminal_cause IS NULL AND termination_observation IS"
    " NULL) OR (status = 'waiting' AND wait_deadline IS NOT NULL AND started_at IS NULL AND "
    "execution_deadline IS NULL AND terminal_at IS NULL AND terminal_cause IS NULL AND "
    "termination_observation IS NULL) OR (status = 'running' AND started_at IS NOT NULL AND "
    "execution_deadline IS NOT NULL AND terminal_at IS NULL AND terminal_cause IS NULL AND "
    "termination_observation IS NULL) OR (status = 'cancellation_requested' AND started_at IS "
    "NOT NULL AND execution_deadline IS NOT NULL AND terminal_at IS NULL AND terminal_cause IS "
    "NOT NULL AND terminal_cause IN ('issue_cancelled', 'issue_deleted', 'execution_deadline', "
    "'owner_lost') AND termination_observation IS NULL) OR (status = 'completed' AND started_at"
    " IS NOT NULL AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL AND "
    "terminal_cause IS NOT NULL AND terminal_cause = 'completed' AND termination_observation IS"
    " NULL) OR (status = 'failed' AND started_at IS NOT NULL AND execution_deadline IS NOT NULL"
    " AND terminal_at IS NOT NULL AND terminal_cause IS NOT NULL AND ((terminal_cause = "
    "'owner_lost' AND termination_observation IS NOT NULL) OR (terminal_cause <> 'owner_lost' "
    "AND termination_observation IS NULL))) OR (status = 'expired' AND terminal_at IS NOT NULL "
    "AND ((started_at IS NULL AND execution_deadline IS NULL AND terminal_cause IS NOT NULL AND"
    " terminal_cause = 'capacity_wait_expired' AND termination_observation IS NULL) OR "
    "(started_at IS NOT NULL AND execution_deadline IS NOT NULL AND terminal_cause IS NOT NULL "
    "AND terminal_cause = 'execution_deadline' AND termination_observation IS NOT NULL))) OR "
    "(status = 'cancelled' AND terminal_at IS NOT NULL AND terminal_cause IS NOT NULL AND "
    "terminal_cause IN ('issue_cancelled', 'issue_deleted', 'lineage_closed') AND ((started_at "
    "IS NULL AND execution_deadline IS NULL AND termination_observation IS NULL) OR (started_at"
    " IS NOT NULL AND execution_deadline IS NOT NULL AND termination_observation IS NOT "
    "NULL)))) IS TRUE"
)


def _replace_shape(shape: str) -> None:
    op.drop_constraint(
        "execution_requests_state_shape_ck", "execution_requests", schema="curie", type_="check"
    )
    op.create_check_constraint(
        "execution_requests_state_shape_ck", "execution_requests", shape, schema="curie"
    )


def upgrade() -> None:
    _replace_shape(_NEW_SHAPE)


def downgrade() -> None:
    connection = op.get_bind()
    if connection.scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM curie.execution_requests "
            "WHERE terminal_cause = 'issue_deleted')"
        )
    ):
        raise RuntimeError("cannot downgrade while deleted-issue cancellation records exist")
    _replace_shape(_OLD_SHAPE)

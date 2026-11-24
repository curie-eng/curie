"""Allow a waiting maintenance successor to settle with a named refusal.

Revision ID: 0197
Revises: 0193
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0197"
down_revision: str = "0193"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "execution_requests"
CONSTRAINT = "execution_requests_state_shape_ck"

_STATE_SHAPE = (
    "((status = 'failed' AND started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_at IS NOT NULL AND terminal_cause IN ('start_failed', 'maintenance_refused') "
    "AND termination_observation IS NULL) "
    "OR (status = 'queued' AND wait_deadline IS NULL AND started_at IS NULL "
    "AND execution_deadline IS NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NULL AND termination_observation IS NULL) "
    "OR (status = 'cancelled' AND wait_deadline IS NULL AND started_at IS NULL "
    "AND execution_deadline IS NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted', 'lineage_closed') "
    "AND termination_observation IS NULL) "
    "OR (status = 'waiting' AND started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_at IS NULL AND terminal_cause IS NULL "
    "AND termination_observation IS NULL) "
    "OR (status = 'running' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NULL AND termination_observation IS NULL) "
    "OR (status = 'cancellation_requested' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted', "
    "'execution_deadline', 'owner_lost', 'enrollment_fenced') "
    "AND termination_observation IS NULL) "
    "OR (status = 'completed' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL "
    "AND terminal_cause IN ('completed', 'questions_asked')) "
    "OR (status = 'failed' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL "
    "AND (terminal_cause NOT IN ('owner_lost', 'enrollment_fenced') "
    "OR termination_observation IS NOT NULL)) "
    "OR (status = 'expired' AND terminal_at IS NOT NULL AND "
    "((started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_cause IS NOT NULL "
    "AND (terminal_cause = 'capacity_wait_expired' "
    "OR (terminal_cause = 'retry_condition_expired' AND blocked_on IS NOT NULL)) "
    "AND termination_observation IS NULL) OR "
    "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
    "AND terminal_cause IS NOT NULL AND terminal_cause = 'execution_deadline' "
    "AND termination_observation IS NOT NULL))) "
    "OR (status = 'cancelled' AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted') AND "
    "((started_at IS NULL AND execution_deadline IS NULL "
    "AND termination_observation IS NULL) OR "
    "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
    "AND termination_observation IS NOT NULL)))) IS TRUE"
)
_PRIOR_SHAPE = _STATE_SHAPE.replace(
    "terminal_cause IN ('start_failed', 'maintenance_refused')",
    "terminal_cause = 'start_failed'",
)


def _replace_state_shape(shape: str) -> None:
    op.execute("SET LOCAL lock_timeout = '15000ms'")
    op.drop_constraint(CONSTRAINT, TABLE, type_="check", schema=SCHEMA)
    op.create_check_constraint(CONSTRAINT, TABLE, shape, schema=SCHEMA)


def upgrade() -> None:
    _replace_state_shape(_STATE_SHAPE)


def downgrade() -> None:
    _replace_state_shape(_PRIOR_SHAPE)

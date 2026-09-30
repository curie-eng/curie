"""Persist revision mentions until the current factory request settles.

Revision ID: 0063
Revises: 0062
Create Date: 2026-09-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0063"
down_revision: str | None = "0062"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "execution_requests"

_OLD_SHAPE = """
((status = 'waiting' AND started_at IS NULL AND execution_deadline IS NULL AND terminal_at
IS NULL AND terminal_cause IS NULL AND termination_observation IS NULL) OR (status =
'running' AND started_at IS NOT NULL AND execution_deadline IS NOT NULL AND terminal_at IS
NULL AND terminal_cause IS NULL AND termination_observation IS NULL) OR (status =
'cancellation_requested' AND started_at IS NOT NULL AND execution_deadline IS NOT NULL AND
terminal_at IS NULL AND terminal_cause IS NOT NULL AND terminal_cause IN
('issue_cancelled', 'execution_deadline', 'owner_lost') AND termination_observation IS
NULL) OR (status = 'completed' AND started_at IS NOT NULL AND execution_deadline IS NOT
NULL AND terminal_at IS NOT NULL AND terminal_cause IS NOT NULL AND terminal_cause =
'completed' AND termination_observation IS NULL) OR (status = 'failed' AND started_at IS
NOT NULL AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL AND terminal_cause
IS NOT NULL AND ((terminal_cause = 'owner_lost' AND termination_observation IS NOT NULL)
OR (terminal_cause <> 'owner_lost' AND termination_observation IS NULL))) OR (status =
'expired' AND terminal_at IS NOT NULL AND ((started_at IS NULL AND execution_deadline IS
NULL AND terminal_cause IS NOT NULL AND terminal_cause = 'capacity_wait_expired' AND
termination_observation IS NULL) OR (started_at IS NOT NULL AND execution_deadline IS NOT
NULL AND terminal_cause IS NOT NULL AND terminal_cause = 'execution_deadline' AND
termination_observation IS NOT NULL))) OR (status = 'cancelled' AND terminal_at IS NOT
NULL AND terminal_cause IS NOT NULL AND terminal_cause = 'issue_cancelled' AND
((started_at IS NULL AND execution_deadline IS NULL AND termination_observation IS NULL)
OR (started_at IS NOT NULL AND execution_deadline IS NOT NULL AND termination_observation
IS NOT NULL)))) IS TRUE
"""

_QUEUE_SHAPE = """(
    status = 'queued' AND wait_deadline IS NULL
    AND started_at IS NULL AND execution_deadline IS NULL
    AND terminal_at IS NULL AND terminal_cause IS NULL
    AND termination_observation IS NULL
)"""
_DISMISSED_SHAPE = """(
    status = 'cancelled' AND wait_deadline IS NULL
    AND started_at IS NULL AND execution_deadline IS NULL
    AND terminal_at IS NOT NULL
    AND terminal_cause IN ('issue_cancelled', 'lineage_closed')
    AND termination_observation IS NULL
)"""

_FUNCTION = """
CREATE OR REPLACE FUNCTION curie.enforce_execution_requests_update_invariants()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
       OR NEW.work_item_id IS DISTINCT FROM OLD.work_item_id
       OR NEW.sequence IS DISTINCT FROM OLD.sequence
       OR (OLD.status <> 'queued'
           AND NEW.wait_deadline IS DISTINCT FROM OLD.wait_deadline)
       OR (OLD.status = 'queued' AND NEW.wait_deadline IS NOT NULL
           AND NEW.status <> 'waiting')
       OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'execution request identity is immutable';
    END IF;

    IF (OLD.started_at IS NULL) <> (OLD.execution_deadline IS NULL) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'stored execution deadlines are inconsistent';
    ELSIF OLD.started_at IS NULL THEN
        IF (NEW.started_at IS NULL) <> (NEW.execution_deadline IS NULL) THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'execution deadlines must be written together';
        END IF;
    ELSIF NEW.started_at IS DISTINCT FROM OLD.started_at
       OR NEW.execution_deadline IS DISTINCT FROM OLD.execution_deadline THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'execution deadlines are write once';
    END IF;

    IF (OLD.objective IS NOT NULL AND NEW.objective IS DISTINCT FROM OLD.objective)
       OR (OLD.requester IS NOT NULL AND NEW.requester IS DISTINCT FROM OLD.requester)
       OR (OLD.reply_kind IS NOT NULL AND NEW.reply_kind IS DISTINCT FROM OLD.reply_kind)
       OR (OLD.reply_address IS NOT NULL
           AND NEW.reply_address IS DISTINCT FROM OLD.reply_address)
       OR (OLD.reply_conversation_id IS NOT NULL
           AND NEW.reply_conversation_id IS DISTINCT FROM OLD.reply_conversation_id) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'execution request snapshot is write once';
    END IF;

    IF NEW.dispatch_generation < OLD.dispatch_generation
       OR NEW.runtime_epoch < OLD.runtime_epoch
       OR NEW.dispatch_epoch < OLD.dispatch_epoch
       OR NEW.capacity_deferrals < OLD.capacity_deferrals
       OR NEW.execution_attempts < OLD.execution_attempts THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'execution request counters never decrease';
    END IF;

    RETURN NEW;
END;
$$
"""


def _old_shape() -> str:
    connection = op.get_bind()
    shape = connection.scalar(
        sa.text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'curie.execution_requests'::regclass "
            "AND conname = 'execution_requests_state_shape_ck'"
        )
    )
    assert isinstance(shape, str) and shape.startswith("CHECK (")
    return shape[len("CHECK (") : -1]


def upgrade() -> None:
    old_shape = _old_shape()
    op.alter_column(TABLE, "wait_deadline", nullable=True, schema=SCHEMA)
    op.drop_constraint("execution_requests_status_ck", TABLE, schema=SCHEMA, type_="check")
    op.create_check_constraint(
        "execution_requests_status_ck",
        TABLE,
        "status IS NOT NULL AND status IN "
        "('queued', 'waiting', 'running', 'cancellation_requested', "
        "'completed', 'failed', 'expired', 'cancelled')",
        schema=SCHEMA,
    )
    op.drop_constraint("execution_requests_state_shape_ck", TABLE, schema=SCHEMA, type_="check")
    op.create_check_constraint(
        "execution_requests_state_shape_ck",
        TABLE,
        f"({_QUEUE_SHAPE} OR {_DISMISSED_SHAPE} OR ({old_shape})) IS TRUE",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "execution_requests_wait_deadline_ck",
        TABLE,
        "status IN ('queued', 'cancelled') OR wait_deadline IS NOT NULL",
        schema=SCHEMA,
    )
    op.execute(_FUNCTION)
    op.create_index(
        "ix_execution_requests_queued",
        TABLE,
        ["work_item_id", "sequence"],
        unique=False,
        schema=SCHEMA,
        postgresql_where=sa.text("status = 'queued'"),
    )


def downgrade() -> None:
    incompatible = op.get_bind().scalar(
        sa.text(
            "SELECT count(*) FROM curie.execution_requests "
            "WHERE wait_deadline IS NULL OR terminal_cause = 'lineage_closed'"
        )
    )
    if incompatible:
        raise RuntimeError("queued revision history prevents migration downgrade")
    op.drop_index("ix_execution_requests_queued", table_name=TABLE, schema=SCHEMA)
    queue_deadline_rule = (
        "OR (OLD.status <> 'queued'\n"
        "           AND NEW.wait_deadline IS DISTINCT FROM OLD.wait_deadline)\n"
        "       OR (OLD.status = 'queued' AND NEW.wait_deadline IS NOT NULL\n"
        "           AND NEW.status <> 'waiting')"
    )
    old_function = _FUNCTION.replace(
        queue_deadline_rule,
        "OR NEW.wait_deadline IS DISTINCT FROM OLD.wait_deadline",
    )
    assert old_function != _FUNCTION
    op.execute(old_function)
    op.drop_constraint("execution_requests_wait_deadline_ck", TABLE, schema=SCHEMA, type_="check")
    op.drop_constraint("execution_requests_state_shape_ck", TABLE, schema=SCHEMA, type_="check")
    op.create_check_constraint(
        "execution_requests_state_shape_ck",
        TABLE,
        _OLD_SHAPE,
        schema=SCHEMA,
    )
    op.drop_constraint("execution_requests_status_ck", TABLE, schema=SCHEMA, type_="check")
    op.create_check_constraint(
        "execution_requests_status_ck",
        TABLE,
        "status IS NOT NULL AND status IN ('waiting', 'running', "
        "'cancellation_requested', 'completed', 'failed', 'expired', 'cancelled')",
        schema=SCHEMA,
    )
    op.alter_column(TABLE, "wait_deadline", nullable=False, schema=SCHEMA)

"""Count start deferrals and allow an unstarted request to fail (#4170).

Revision ID: 0092
Revises: 0091
Create Date: 2026-10-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0092"
down_revision: str | None = "0091"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "execution_requests"
SHAPE = "execution_requests_state_shape_ck"

_START_FAILED_SHAPE = """(
    status = 'failed'
    AND started_at IS NULL AND execution_deadline IS NULL
    AND terminal_at IS NOT NULL
    AND terminal_cause = 'start_failed'
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
       OR NEW.start_deferrals < OLD.start_deferrals
       OR NEW.execution_attempts < OLD.execution_attempts THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'execution request counters never decrease';
    END IF;

    RETURN NEW;
END;
$$
"""
_START_DEFERRALS_RULE = "       OR NEW.start_deferrals < OLD.start_deferrals\n"


def _shape() -> str:
    """Return the live state-shape expression without its ``CHECK (...)`` wrapper."""
    shape = op.get_bind().scalar(
        sa.text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'curie.execution_requests'::regclass "
            f"AND conname = '{SHAPE}'"
        )
    )
    assert isinstance(shape, str) and shape.startswith("CHECK (") and shape.endswith(")")
    return shape[len("CHECK (") : -1]


def _deparse(expression: str) -> str:
    """Return how Postgres deparses ``expression`` as a check on this table."""
    probe = "execution_requests_state_shape_probe"
    bind = op.get_bind()
    bind.execute(
        sa.text(
            f"ALTER TABLE {SCHEMA}.{TABLE} ADD CONSTRAINT {probe} CHECK ({expression}) NOT VALID"
        )
    )
    deparsed = bind.scalar(
        sa.text(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = 'curie.execution_requests'::regclass "
            f"AND conname = '{probe}'"
        )
    )
    bind.execute(sa.text(f"ALTER TABLE {SCHEMA}.{TABLE} DROP CONSTRAINT {probe}"))
    assert isinstance(deparsed, str) and deparsed.startswith("CHECK (")
    assert deparsed.endswith(") NOT VALID"), deparsed
    return deparsed[len("CHECK (") : -len(") NOT VALID")]


def _replace_shape(expression: str) -> None:
    op.drop_constraint(SHAPE, TABLE, schema=SCHEMA, type_="check")
    op.create_check_constraint(SHAPE, TABLE, expression, schema=SCHEMA)


def _widened(prior: str) -> str:
    return f"({_START_FAILED_SHAPE} OR ({prior})) IS TRUE"


def upgrade() -> None:
    prior = _shape()
    op.add_column(
        TABLE,
        sa.Column("start_deferrals", sa.Integer(), nullable=False, server_default="0"),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "execution_requests_start_deferrals_ck",
        TABLE,
        "start_deferrals >= 0",
        schema=SCHEMA,
    )
    _replace_shape(_widened(prior))
    op.execute(_FUNCTION)


def downgrade() -> None:
    incompatible = op.get_bind().scalar(
        sa.text(
            "SELECT count(*) FROM curie.execution_requests WHERE terminal_cause = 'start_failed'"
        )
    )
    if incompatible:
        raise RuntimeError("start_failed execution requests prevent migration downgrade")

    # The live shape is ``((<start failed> OR (<prior>)) IS TRUE)`` as Postgres
    # deparsed it. Deparse the start-failed clause on its own to find the
    # prefix to strip, then prove the stripped prior rebuilds the live
    # definition exactly before restoring it.
    widened = _shape()
    marker = "start_deferrals IS NULL"
    probe = _deparse(f"({_START_FAILED_SHAPE} OR ({marker})) IS TRUE")
    probe_suffix = f" OR ({marker})) IS TRUE)"
    assert probe.endswith(probe_suffix), probe
    prefix = probe[: -len(probe_suffix)] + " OR "
    suffix = ") IS TRUE)"
    assert widened.startswith(prefix) and widened.endswith(suffix), widened
    prior = widened[len(prefix) : -len(suffix)]
    if _deparse(_widened(prior)) != widened:
        raise RuntimeError("execution request state shape did not round trip")

    old_function = _FUNCTION.replace(_START_DEFERRALS_RULE, "")
    assert old_function != _FUNCTION
    op.execute(old_function)
    _replace_shape(prior)
    op.drop_constraint("execution_requests_start_deferrals_ck", TABLE, schema=SCHEMA, type_="check")
    op.drop_column(TABLE, "start_deferrals", schema=SCHEMA)

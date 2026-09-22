"""WorkItem dispatch, ownership, and reply snapshot columns.

Revision ID: 0047
Revises: 0046
Create Date: 2026-09-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0047"
down_revision: str | None = "0046"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"

_STATE_SHAPE_0046 = """\
((status = 'waiting' AND started_at IS NULL \
AND execution_deadline IS NULL AND terminal_at IS NULL \
AND terminal_cause IS NULL AND termination_observation IS NULL) \
OR (status = 'running' AND started_at IS NOT NULL \
AND execution_deadline IS NOT NULL AND terminal_at IS NULL \
AND terminal_cause IS NULL AND termination_observation IS NULL) \
OR (status = 'cancellation_requested' AND started_at IS NOT NULL \
AND execution_deadline IS NOT NULL AND terminal_at IS NULL \
AND terminal_cause IS NOT NULL \
AND terminal_cause IN ('issue_cancelled', 'execution_deadline') \
AND termination_observation IS NULL) \
OR (status = 'completed' AND started_at IS NOT NULL \
AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL \
AND terminal_cause IS NOT NULL AND terminal_cause = 'completed' \
AND termination_observation IS NULL) \
OR (status = 'failed' AND started_at IS NOT NULL \
AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL \
AND terminal_cause IS NOT NULL AND termination_observation IS NULL) \
OR (status = 'expired' AND terminal_at IS NOT NULL AND \
((started_at IS NULL AND execution_deadline IS NULL \
AND terminal_cause IS NOT NULL \
AND terminal_cause = 'capacity_wait_expired' \
AND termination_observation IS NULL) OR \
(started_at IS NOT NULL AND execution_deadline IS NOT NULL \
AND terminal_cause IS NOT NULL \
AND terminal_cause = 'execution_deadline' \
AND termination_observation IS NOT NULL))) \
OR (status = 'cancelled' AND terminal_at IS NOT NULL \
AND terminal_cause IS NOT NULL \
AND terminal_cause = 'issue_cancelled' AND \
((started_at IS NULL AND execution_deadline IS NULL \
AND termination_observation IS NULL) OR \
(started_at IS NOT NULL AND execution_deadline IS NOT NULL \
AND termination_observation IS NOT NULL)))) IS TRUE\
"""

_STATE_SHAPE_0047 = """\
((status = 'waiting' AND started_at IS NULL \
AND execution_deadline IS NULL AND terminal_at IS NULL \
AND terminal_cause IS NULL AND termination_observation IS NULL) \
OR (status = 'running' AND started_at IS NOT NULL \
AND execution_deadline IS NOT NULL AND terminal_at IS NULL \
AND terminal_cause IS NULL AND termination_observation IS NULL) \
OR (status = 'cancellation_requested' AND started_at IS NOT NULL \
AND execution_deadline IS NOT NULL AND terminal_at IS NULL \
AND terminal_cause IS NOT NULL \
AND terminal_cause IN ('issue_cancelled', 'execution_deadline', 'owner_lost') \
AND termination_observation IS NULL) \
OR (status = 'completed' AND started_at IS NOT NULL \
AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL \
AND terminal_cause IS NOT NULL AND terminal_cause = 'completed' \
AND termination_observation IS NULL) \
OR (status = 'failed' AND started_at IS NOT NULL \
AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL \
AND terminal_cause IS NOT NULL AND \
((terminal_cause = 'owner_lost' AND termination_observation IS NOT NULL) \
OR (terminal_cause <> 'owner_lost' AND termination_observation IS NULL))) \
OR (status = 'expired' AND terminal_at IS NOT NULL AND \
((started_at IS NULL AND execution_deadline IS NULL \
AND terminal_cause IS NOT NULL \
AND terminal_cause = 'capacity_wait_expired' \
AND termination_observation IS NULL) OR \
(started_at IS NOT NULL AND execution_deadline IS NOT NULL \
AND terminal_cause IS NOT NULL \
AND terminal_cause = 'execution_deadline' \
AND termination_observation IS NOT NULL))) \
OR (status = 'cancelled' AND terminal_at IS NOT NULL \
AND terminal_cause IS NOT NULL \
AND terminal_cause = 'issue_cancelled' AND \
((started_at IS NULL AND execution_deadline IS NULL \
AND termination_observation IS NULL) OR \
(started_at IS NOT NULL AND execution_deadline IS NOT NULL \
AND termination_observation IS NOT NULL)))) IS TRUE\
"""

_FUNCTION_0046 = """
        CREATE OR REPLACE FUNCTION curie.enforce_execution_requests_update_invariants()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.work_item_id IS DISTINCT FROM OLD.work_item_id
               OR NEW.sequence IS DISTINCT FROM OLD.sequence
               OR NEW.wait_deadline IS DISTINCT FROM OLD.wait_deadline
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

            RETURN NEW;
        END;
        $$
"""

_FUNCTION_0047 = """
        CREATE OR REPLACE FUNCTION curie.enforce_execution_requests_update_invariants()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.work_item_id IS DISTINCT FROM OLD.work_item_id
               OR NEW.sequence IS DISTINCT FROM OLD.sequence
               OR NEW.wait_deadline IS DISTINCT FROM OLD.wait_deadline
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

_NEW_CHECK_CONSTRAINTS = (
    (
        "execution_requests_dispatch_generation_ck",
        "dispatch_generation >= 1",
    ),
    (
        "execution_requests_published_generation_ck",
        "published_generation IS NULL OR "
        "published_generation BETWEEN 1 AND dispatch_generation",
    ),
    (
        "execution_requests_acquired_generation_ck",
        "acquired_generation IS NULL OR "
        "acquired_generation BETWEEN 1 AND dispatch_generation",
    ),
    (
        "execution_requests_capacity_deferrals_ck",
        "capacity_deferrals >= 0",
    ),
    (
        "execution_requests_dispatch_epoch_ck",
        "dispatch_epoch >= 0",
    ),
    (
        "execution_requests_runtime_epoch_ck",
        "runtime_epoch >= 0",
    ),
    (
        "execution_requests_execution_attempts_ck",
        "execution_attempts IN (0, 1) AND "
        "(execution_attempts = 1) = (started_at IS NOT NULL)",
    ),
    (
        "execution_requests_snapshot_ck",
        "(objective IS NULL AND requester IS NULL AND reply_kind IS NULL "
        "AND reply_address IS NULL AND reply_conversation_id IS NULL) OR "
        "(objective IS NOT NULL AND requester IS NOT NULL AND "
        "reply_kind IS NOT NULL AND reply_address IS NOT NULL AND "
        "reply_conversation_id IS NOT NULL AND length(btrim(objective)) > 0 "
        "AND length(objective) <= 65536 AND length(btrim(requester)) > 0 "
        "AND length(btrim(reply_kind)) > 0 AND length(btrim(reply_address)) > 0 "
        "AND length(btrim(reply_conversation_id)) > 0)",
    ),
)


def upgrade() -> None:
    op.add_column(
        "execution_requests",
        sa.Column(
            "dispatch_generation",
            sa.Integer(),
            server_default="1",
            nullable=False,
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("published_generation", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column(
            "dispatch_not_before",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("dispatch_owner", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column(
            "dispatch_epoch",
            sa.BigInteger(),
            server_default="0",
            nullable=False,
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column(
            "dispatch_lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("acquired_generation", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("acquire_owner", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("acquire_expires_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column(
            "capacity_deferrals",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("last_deferral_reason", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column(
            "execution_attempts",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("runtime_owner", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column(
            "runtime_epoch",
            sa.BigInteger(),
            server_default="0",
            nullable=False,
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column(
            "runtime_heartbeat_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column(
            "terminate_published_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("runtime_claim_name", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("runtime_sandbox_name", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("objective", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("requester", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("reply_kind", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("reply_address", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("reply_conversation_id", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.execute(
        sa.text(
            f"UPDATE {SCHEMA}.execution_requests "
            "SET execution_attempts = 1 WHERE started_at IS NOT NULL"
        )
    )

    op.drop_constraint(
        "execution_requests_state_shape_ck",
        "execution_requests",
        schema=SCHEMA,
        type_="check",
    )
    op.create_check_constraint(
        "execution_requests_state_shape_ck",
        "execution_requests",
        _STATE_SHAPE_0047,
        schema=SCHEMA,
    )
    for name, sql in _NEW_CHECK_CONSTRAINTS:
        op.create_check_constraint(
            name,
            "execution_requests",
            sql,
            schema=SCHEMA,
        )

    op.create_index(
        "ix_execution_requests_dispatch_due",
        "execution_requests",
        ["dispatch_not_before"],
        schema=SCHEMA,
        postgresql_where=sa.text("status = 'waiting'"),
    )
    op.create_index(
        "ix_execution_requests_runtime_liveness",
        "execution_requests",
        ["runtime_heartbeat_expires_at"],
        schema=SCHEMA,
        postgresql_where=sa.text(
            "status IN ('running','cancellation_requested')"
        ),
    )
    op.execute(_FUNCTION_0047)


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM curie.execution_requests
                WHERE terminal_cause = 'owner_lost'
            ) THEN
                RAISE EXCEPTION
                    'cannot downgrade 0047 while owner_lost rows exist';
            END IF;
        END
        $$;
        """
    )
    op.execute(_FUNCTION_0046)
    op.drop_constraint(
        "execution_requests_state_shape_ck",
        "execution_requests",
        schema=SCHEMA,
        type_="check",
    )
    op.create_check_constraint(
        "execution_requests_state_shape_ck",
        "execution_requests",
        _STATE_SHAPE_0046,
        schema=SCHEMA,
    )
    for name, _sql in _NEW_CHECK_CONSTRAINTS:
        op.drop_constraint(
            name,
            "execution_requests",
            schema=SCHEMA,
            type_="check",
        )
    op.drop_index(
        "ix_execution_requests_runtime_liveness",
        table_name="execution_requests",
        schema=SCHEMA,
    )
    op.drop_index(
        "ix_execution_requests_dispatch_due",
        table_name="execution_requests",
        schema=SCHEMA,
    )
    for column in (
        "reply_conversation_id",
        "reply_address",
        "reply_kind",
        "requester",
        "objective",
        "runtime_sandbox_name",
        "runtime_claim_name",
        "terminate_published_at",
        "runtime_heartbeat_expires_at",
        "runtime_epoch",
        "runtime_owner",
        "execution_attempts",
        "last_deferral_reason",
        "capacity_deferrals",
        "acquire_expires_at",
        "acquire_owner",
        "acquired_generation",
        "dispatch_lease_expires_at",
        "dispatch_epoch",
        "dispatch_owner",
        "dispatch_not_before",
        "published_generation",
        "dispatch_generation",
    ):
        op.drop_column("execution_requests", column, schema=SCHEMA)

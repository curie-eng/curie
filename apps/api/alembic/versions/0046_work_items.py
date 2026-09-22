"""Durable work items and bounded execution requests.

Revision ID: 0046
Revises: 0045
Create Date: 2026-09-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0046"
down_revision: str | None = "0045"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "work_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("github_repository_id", sa.BigInteger(), nullable=False),
        sa.Column("github_issue_number", sa.Integer(), nullable=False),
        sa.Column("github_installation_id", sa.BigInteger(), nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("repo_full_name", sa.String(), nullable=False),
        sa.Column("conversation_id", sa.String(), nullable=False),
        sa.Column(
            "publication_lineage_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("next_sequence", sa.Integer(), server_default="1", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "github_repository_id > 0",
            name="work_items_github_repository_id_ck",
        ),
        sa.CheckConstraint(
            "github_issue_number > 0",
            name="work_items_github_issue_number_ck",
        ),
        sa.CheckConstraint(
            "github_installation_id > 0",
            name="work_items_github_installation_id_ck",
        ),
        sa.CheckConstraint("version >= 1", name="work_items_version_ck"),
        sa.CheckConstraint(
            "next_sequence >= 1",
            name="work_items_next_sequence_ck",
        ),
        sa.ForeignKeyConstraint(
            ["agent_id"],
            [f"{SCHEMA}.agents.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["publication_lineage_id"],
            [f"{SCHEMA}.thread_publication_lineages.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "github_repository_id",
            "github_issue_number",
            name="work_items_github_issue_key",
        ),
        sa.UniqueConstraint(
            "publication_lineage_id",
            name="work_items_publication_lineage_key",
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "execution_requests",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("work_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), server_default="waiting", nullable=False),
        sa.Column("wait_deadline", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("execution_deadline", sa.DateTime(timezone=True), nullable=True),
        sa.Column("terminal_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("terminal_cause", sa.String(), nullable=True),
        sa.Column("termination_observation", sa.Text(), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "sequence > 0",
            name="execution_requests_sequence_ck",
        ),
        sa.CheckConstraint(
            "version >= 1",
            name="execution_requests_version_ck",
        ),
        sa.CheckConstraint(
            "status IS NOT NULL AND status IN "
            "('waiting', 'running', 'cancellation_requested', 'completed', "
            "'failed', 'expired', 'cancelled')",
            name="execution_requests_status_ck",
        ),
        sa.CheckConstraint(
            "terminal_cause IS NULL OR length(btrim(terminal_cause)) > 0",
            name="execution_requests_terminal_cause_ck",
        ),
        sa.CheckConstraint(
            "termination_observation IS NULL "
            "OR length(btrim(termination_observation)) > 0",
            name="execution_requests_termination_observation_ck",
        ),
        sa.CheckConstraint(
            "(started_at IS NULL AND execution_deadline IS NULL) OR "
            "(started_at IS NOT NULL AND execution_deadline IS NOT NULL AND "
            "execution_deadline = started_at + interval '1800 seconds')",
            name="execution_requests_deadline_ck",
        ),
        sa.CheckConstraint(
            "((status = 'waiting' AND started_at IS NULL "
            "AND execution_deadline IS NULL AND terminal_at IS NULL "
            "AND terminal_cause IS NULL AND termination_observation IS NULL) "
            "OR (status = 'running' AND started_at IS NOT NULL "
            "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
            "AND terminal_cause IS NULL AND termination_observation IS NULL) "
            "OR (status = 'cancellation_requested' AND started_at IS NOT NULL "
            "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
            "AND terminal_cause IS NOT NULL "
            "AND terminal_cause IN ('issue_cancelled', 'execution_deadline') "
            "AND termination_observation IS NULL) "
            "OR (status = 'completed' AND started_at IS NOT NULL "
            "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
            "AND terminal_cause IS NOT NULL AND terminal_cause = 'completed' "
            "AND termination_observation IS NULL) "
            "OR (status = 'failed' AND started_at IS NOT NULL "
            "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
            "AND terminal_cause IS NOT NULL AND termination_observation IS NULL) "
            "OR (status = 'expired' AND terminal_at IS NOT NULL AND "
            "((started_at IS NULL AND execution_deadline IS NULL "
            "AND terminal_cause IS NOT NULL "
            "AND terminal_cause = 'capacity_wait_expired' "
            "AND termination_observation IS NULL) OR "
            "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
            "AND terminal_cause IS NOT NULL "
            "AND terminal_cause = 'execution_deadline' "
            "AND termination_observation IS NOT NULL))) "
            "OR (status = 'cancelled' AND terminal_at IS NOT NULL "
            "AND terminal_cause IS NOT NULL "
            "AND terminal_cause = 'issue_cancelled' AND "
            "((started_at IS NULL AND execution_deadline IS NULL "
            "AND termination_observation IS NULL) OR "
            "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
            "AND termination_observation IS NOT NULL)))) IS TRUE",
            name="execution_requests_state_shape_ck",
        ),
        sa.ForeignKeyConstraint(
            ["work_item_id"],
            [f"{SCHEMA}.work_items.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "work_item_id",
            "sequence",
            name="execution_requests_work_item_sequence_key",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "uq_execution_requests_active_work_item",
        "execution_requests",
        ["work_item_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text(
            "status IN ('waiting', 'running', 'cancellation_requested')"
        ),
    )

    op.execute(
        """
        CREATE FUNCTION curie.enforce_work_items_update_invariants()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.github_repository_id IS DISTINCT FROM OLD.github_repository_id
               OR NEW.github_issue_number IS DISTINCT FROM OLD.github_issue_number
               OR NEW.github_installation_id IS DISTINCT FROM OLD.github_installation_id
               OR NEW.agent_id IS DISTINCT FROM OLD.agent_id
               OR NEW.repo_full_name IS DISTINCT FROM OLD.repo_full_name
               OR NEW.conversation_id IS DISTINCT FROM OLD.conversation_id
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'work item identity is immutable';
            END IF;

            IF OLD.cancelled_at IS NOT NULL
               AND NEW.cancelled_at IS DISTINCT FROM OLD.cancelled_at THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'work item cancellation is sticky';
            END IF;

            IF OLD.publication_lineage_id IS NOT NULL
               AND NEW.publication_lineage_id IS DISTINCT FROM OLD.publication_lineage_id THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'work item publication lineage is write once';
            END IF;

            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER work_items_update_invariants
        BEFORE UPDATE ON curie.work_items
        FOR EACH ROW
        EXECUTE FUNCTION curie.enforce_work_items_update_invariants()
        """
    )

    op.execute(
        """
        CREATE FUNCTION curie.enforce_execution_requests_update_invariants()
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
    )
    op.execute(
        """
        CREATE TRIGGER execution_requests_update_invariants
        BEFORE UPDATE ON curie.execution_requests
        FOR EACH ROW
        EXECUTE FUNCTION curie.enforce_execution_requests_update_invariants()
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS execution_requests_update_invariants "
        "ON curie.execution_requests"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS work_items_update_invariants ON curie.work_items"
    )
    op.execute("DROP FUNCTION IF EXISTS curie.enforce_execution_requests_update_invariants()")
    op.execute("DROP FUNCTION IF EXISTS curie.enforce_work_items_update_invariants()")
    op.drop_table("execution_requests", schema=SCHEMA)
    op.drop_table("work_items", schema=SCHEMA)

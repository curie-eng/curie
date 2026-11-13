"""Bounded read-only factory CI observations (#5430).

Revision ID: 0163
Revises: 0160
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0163"
down_revision: str = "0160"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "factory_ci_observations",
        sa.Column("request_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("work_item_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("lineage_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("head_sha", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="observing", nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("next_due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.PrimaryKeyConstraint("request_id"),
        sa.ForeignKeyConstraint(
            ["request_id"], ["curie.execution_requests.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["work_item_id"], ["curie.work_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["lineage_id"], ["curie.thread_publication_lineages.id"], ondelete="CASCADE"
        ),
        sa.CheckConstraint(
            "status IN ('observing', 'completed', 'expired', 'invalidated')",
            name="factory_ci_observations_status_ck",
        ),
        sa.CheckConstraint(
            "expires_at = created_at + interval '86400 seconds'",
            name="factory_ci_observations_window_ck",
        ),
        sa.CheckConstraint("version >= 1", name="factory_ci_observations_version_ck"),
        sa.CheckConstraint("length(head_sha) > 0", name="factory_ci_observations_head_ck"),
        sa.CheckConstraint(
            "(status = 'completed') = (observed_at IS NOT NULL)",
            name="factory_ci_observations_proof_ck",
        ),
        schema="curie",
    )
    op.create_index(
        "factory_ci_observations_work_item_id_idx",
        "factory_ci_observations",
        ["work_item_id"],
        schema="curie",
    )
    op.create_index(
        "factory_ci_observations_due_idx",
        "factory_ci_observations",
        ["next_due_at"],
        schema="curie",
        postgresql_where=sa.text("status = 'observing'"),
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '15s'")
    op.execute("LOCK TABLE curie.factory_ci_observations IN ACCESS EXCLUSIVE MODE")
    if (
        op.get_bind()
        .exec_driver_sql("SELECT EXISTS (SELECT 1 FROM curie.factory_ci_observations)")
        .scalar_one()
    ):
        raise RuntimeError("factory_ci_observation_records_present")
    op.drop_table("factory_ci_observations", schema="curie")

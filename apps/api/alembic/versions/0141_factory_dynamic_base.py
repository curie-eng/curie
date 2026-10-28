"""Resolve factory bases per request and reconcile durable base moves.

Revision ID: 0141
Revises: 0139
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0141"
down_revision: str = "0139"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"


def upgrade() -> None:
    op.add_column(
        "work_items",
        sa.Column("base_resolved_request_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=S,
    )
    op.add_column(
        "work_items", sa.Column("base_selection_fingerprint", sa.Text(), nullable=True), schema=S
    )
    op.add_column(
        "work_items",
        sa.Column("readmit_base_settle_until", sa.DateTime(timezone=True), nullable=True),
        schema=S,
    )
    op.add_column(
        "work_items", sa.Column("base_requested_branch", sa.Text(), nullable=True), schema=S
    )
    op.add_column(
        "work_items", sa.Column("base_requested_source", sa.Text(), nullable=True), schema=S
    )
    op.add_column(
        "execution_requests",
        sa.Column("base_settle_until", sa.DateTime(timezone=True), nullable=True),
        schema=S,
    )
    op.create_table(
        "factory_base_transitions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "work_item_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.work_items.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "request_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.execution_requests.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "lineage_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.thread_publication_lineages.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("phase", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("owner", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("repository_snapshot", postgresql.JSONB(), nullable=True),
        *[
            sa.Column(name, sa.Text(), nullable=False)
            for name in (
                "old_base_ref",
                "old_base_sha",
                "old_head_sha",
                "target_base_ref",
                "target_base_sha",
                "base_source",
                "branch",
                "pull_request_number",
                "committer_name",
                "committer_email",
            )
        ],
        sa.Column("committer_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rebased_head_sha", sa.Text(), nullable=True),
        sa.Column("verification_head_sha", sa.Text(), nullable=True),
        sa.Column("changed_paths", postgresql.JSONB(), nullable=False, server_default="[]"),
        *[
            sa.Column(name, sa.DateTime(timezone=True), nullable=True)
            for name in (
                "prepared_at",
                "pushed_at",
                "retargeted_at",
                "ci_pushed_at",
                "applied_at",
                "outcome_ready_at",
                "ci_ready_at",
                "ci_settled_at",
                "refusal_noticed_at",
            )
        ],
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "phase IN ('pending','prepared','pushed','retargeted','ci_prepared',"
            "'ci_pushed','applied','conflict','refused')",
            name="factory_base_transitions_phase_ck",
        ),
        sa.CheckConstraint(
            "version >= 1 AND attempts >= 0", name="factory_base_transitions_version_ck"
        ),
        sa.CheckConstraint(
            "base_source IN ('label','default')", name="factory_base_transitions_source_ck"
        ),
        schema=S,
    )
    for field in ("work_item_id", "lineage_id"):
        suffix = "item" if field == "work_item_id" else "lineage"
        op.create_index(
            f"uq_factory_base_transitions_active_{suffix}",
            "factory_base_transitions",
            [field],
            unique=True,
            postgresql_where=sa.text("phase NOT IN ('applied','conflict','refused')"),
            schema=S,
        )


def downgrade() -> None:
    if op.get_bind().scalar(
        sa.text("SELECT EXISTS (SELECT 1 FROM curie.factory_base_transitions)")
    ):
        raise RuntimeError("factory_base_transition_history_present")
    op.drop_table("factory_base_transitions", schema=S)
    op.drop_column("execution_requests", "base_settle_until", schema=S)
    for name in (
        "base_resolved_request_id",
        "base_selection_fingerprint",
        "readmit_base_settle_until",
        "base_requested_branch",
        "base_requested_source",
    ):
        op.drop_column("work_items", name, schema=S)

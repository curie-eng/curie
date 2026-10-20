"""Learned skills (ADR 0237, epic #4780).

Adds ``agents.learned_skills BOOLEAN NOT NULL DEFAULT false`` (every existing
agent comes out of the upgrade with it off), the ``learned_skills`` head table
and the insert-only ``learned_skill_revisions`` history. The head row carries a
skill's current content and its ``revision`` number, the compare-and-set target
of every change. Only one ACTIVE skill per (agent, slug); a deleted one keeps
its row and may share the slug.

Expand-only: one defaulted column and two new, empty tables.

Revision ID: selfimp_skills_0001
Revises: selfimp_safety_0001
Create Date: 2026-10-20
"""

from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "selfimp_skills_0001"
down_revision: str | None = "selfimp_safety_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def _timestamp(name: str) -> sa.Column[datetime]:
    return sa.Column(
        name, sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column("learned_skills", sa.Boolean(), server_default=sa.false(), nullable=False),
        schema=SCHEMA,
    )
    op.create_table(
        "learned_skills",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "agent_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        schema=SCHEMA,
    )
    op.create_index(
        "uq_learned_skills_active_slug",
        "learned_skills",
        ["agent_id", "slug"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_table(
        "learned_skill_revisions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "skill_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.learned_skills.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("digest", sa.Text(), nullable=False),
        # The skill's resulting state after this revision, so reverting to a
        # tombstone restores the deletion too.
        sa.Column("deleted", sa.Boolean(), nullable=False),
        sa.Column("reverted_to", sa.Integer(), nullable=True),
        sa.Column("author", sa.Text(), nullable=False),
        sa.Column("conversation", sa.Text(), nullable=True),
        sa.Column("turn", sa.Text(), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        _timestamp("created_at"),
        sa.CheckConstraint(
            "action IN ('create', 'patch', 'revert', 'delete')",
            name="learned_skill_revisions_action_ck",
        ),
        sa.CheckConstraint(
            "source IN ('agent', 'operator')", name="learned_skill_revisions_source_ck"
        ),
        sa.UniqueConstraint("skill_id", "revision", name="uq_learned_skill_revisions_revision"),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("learned_skill_revisions", schema=SCHEMA)
    op.drop_index("uq_learned_skills_active_slug", table_name="learned_skills", schema=SCHEMA)
    op.drop_table("learned_skills", schema=SCHEMA)
    op.drop_column("agents", "learned_skills", schema=SCHEMA)

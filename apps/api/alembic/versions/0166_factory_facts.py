"""Cached facts and metadata-only request delivery (ADR 0280).

Revision ID: 0166
Revises: 0163
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0166"
down_revision: str = "0163"
branch_labels: None = None
depends_on: None = None


def upgrade() -> None:
    op.create_table(
        "factory_fact_observations",
        sa.Column("kind", sa.Text(), primary_key=True),
        sa.Column(
            "agent_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.agents.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("scope", sa.Text(), primary_key=True),
        sa.Column("item", sa.Text(), primary_key=True),
        sa.Column("subject_digest", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "kind IN ('tool', 'model_credit')", name="factory_fact_observation_kind_ck"
        ),
        sa.CheckConstraint(
            "subject_digest ~ '^sha256:[0-9a-f]{64}$'", name="factory_fact_observation_digest_ck"
        ),
        schema="curie",
    )
    op.create_table(
        "factory_facts_requests",
        sa.Column("tracker_host", sa.Text(), primary_key=True),
        sa.Column("repository_id", sa.Text(), primary_key=True),
        sa.Column("issue_number", sa.BigInteger(), primary_key=True),
        sa.Column("trigger_label", sa.Text(), primary_key=True),
        sa.Column("request_comment_id", sa.BigInteger(), primary_key=True),
        sa.Column("digest", sa.Text(), nullable=False),
        sa.Column("actor_id", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("answer_comment_id", sa.Text(), nullable=True),
        sa.Column("answered_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("state IN ('owed', 'answered')", name="factory_facts_request_state_ck"),
        schema="curie",
    )
    op.create_index(
        "ix_factory_facts_actor_window",
        "factory_facts_requests",
        ["tracker_host", "repository_id", "actor_id", "answered_at"],
        schema="curie",
    )


def downgrade() -> None:
    op.drop_table("factory_facts_requests", schema="curie")
    op.drop_table("factory_fact_observations", schema="curie")

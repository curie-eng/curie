"""initial agents, agent_versions, deployments

Revision ID: 0001
Revises:
Create Date: 2026-07-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

    environment = postgresql.ENUM(
        "prod", "dev", name="environment", schema=SCHEMA, create_type=False
    )
    environment.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "agents",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(), nullable=False, unique=True),
        sa.Column("slack_channel", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "agent_versions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_label", sa.String(), nullable=False),
        sa.Column("bundle_ref", sa.String(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        schema=SCHEMA,
    )
    op.create_index("ix_agent_versions_agent_id", "agent_versions", ["agent_id"], schema=SCHEMA)

    op.create_table(
        "deployments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("environment", environment, nullable=False),
        sa.Column("status", sa.String(), server_default="active", nullable=False),
        sa.Column(
            "deployed_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["version_id"],
            [f"{SCHEMA}.agent_versions.id"],
            ondelete="CASCADE",
        ),
        schema=SCHEMA,
    )
    op.create_index("ix_deployments_agent_id", "deployments", ["agent_id"], schema=SCHEMA)
    op.create_index("ix_deployments_version_id", "deployments", ["version_id"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_table("deployments", schema=SCHEMA)
    op.drop_table("agent_versions", schema=SCHEMA)
    op.drop_table("agents", schema=SCHEMA)
    postgresql.ENUM(name="environment", schema=SCHEMA).drop(op.get_bind(), checkfirst=True)
    op.execute(f"DROP SCHEMA IF EXISTS {SCHEMA}")

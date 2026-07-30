"""add git-flow columns: repo_full_name, commit_sha, bot_identity

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column("repo_full_name", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_agents_repo_full_name",
        "agents",
        ["repo_full_name"],
        unique=True,
        schema=SCHEMA,
    )

    op.add_column(
        "agent_versions",
        sa.Column("commit_sha", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_agent_versions_commit_sha",
        "agent_versions",
        ["commit_sha"],
        schema=SCHEMA,
    )

    op.add_column(
        "deployments",
        sa.Column("bot_identity", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "deployments",
        sa.Column("commit_sha", sa.String(), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("deployments", "commit_sha", schema=SCHEMA)
    op.drop_column("deployments", "bot_identity", schema=SCHEMA)
    op.drop_index("ix_agent_versions_commit_sha", "agent_versions", schema=SCHEMA)
    op.drop_column("agent_versions", "commit_sha", schema=SCHEMA)
    op.drop_index("ix_agents_repo_full_name", "agents", schema=SCHEMA)
    op.drop_column("agents", "repo_full_name", schema=SCHEMA)

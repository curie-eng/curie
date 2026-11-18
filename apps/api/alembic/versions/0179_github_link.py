"""Add self-service GitHub link state and identity display labels (#5719).

Revision ID: 0179
Revises: 0178
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0179"
down_revision: str = "0178"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "identity_links", sa.Column("display_label", sa.String(), nullable=True), schema="curie"
    )
    op.create_table(
        "github_link_states",
        sa.Column("state_hash", sa.String(64), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["curie.tenants.id"]),
        sa.ForeignKeyConstraint(
            ["tenant_id", "principal_id"],
            ["curie.principals.tenant_id", "curie.principals.id"],
            name="github_link_states_principal_fkey",
        ),
        schema="curie",
    )


def downgrade() -> None:
    op.drop_table("github_link_states", schema="curie")
    op.drop_column("identity_links", "display_label", schema="curie")

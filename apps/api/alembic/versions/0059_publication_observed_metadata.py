"""Store observed pull request metadata for publication time conflict checks."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0059"
down_revision: str | None = "0058"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "publications",
        sa.Column("observed_title_sha256", sa.String(), nullable=True),
        schema="curie",
    )
    op.add_column(
        "publications",
        sa.Column("observed_body_sha256", sa.String(), nullable=True),
        schema="curie",
    )
    op.add_column(
        "publications",
        sa.Column("metadata_updated_at", sa.DateTime(timezone=True), nullable=True),
        schema="curie",
    )


def downgrade() -> None:
    op.drop_column("publications", "metadata_updated_at", schema="curie")
    op.drop_column("publications", "observed_body_sha256", schema="curie")
    op.drop_column("publications", "observed_title_sha256", schema="curie")

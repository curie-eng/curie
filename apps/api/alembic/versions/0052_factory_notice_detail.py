"""Carry the provider's own failure message on a factory notice (#3073).

A failed factory run used to comment only its terminal cause code. The notice
now also keeps the provider's message, redacted of keys and tokens before it is
stored, so the operator reading the issue sees why the model call failed.

Revision ID: 0052
Revises: 0051
Create Date: 2026-09-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0052"
down_revision: str | None = "0051"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.add_column(
        "factory_terminal_notices",
        sa.Column("detail", sa.Text(), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("factory_terminal_notices", "detail", schema=SCHEMA)

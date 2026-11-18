"""Capture Slack account labels for personal plan connection (#5718).

Revision ID: 0180
Revises: 0179
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0180"
down_revision: str = "0179"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
_COLUMNS = ("slack_display_name", "slack_handle", "slack_workspace_name")


def upgrade() -> None:
    for name in _COLUMNS:
        op.add_column(
            "identity_connect_tokens", sa.Column(name, sa.Text(), nullable=True), schema="curie"
        )


def downgrade() -> None:
    for name in reversed(_COLUMNS):
        op.drop_column("identity_connect_tokens", name, schema="curie")

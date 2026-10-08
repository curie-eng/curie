"""Join the published stable and feature-train migration histories.

Revision ID: 0099
Revises: 0098, 0093
Create Date: 2026-10-08

Both parents remain intact. The first parent retains the feature serving
minimum ancestry; Alembic applies every missing ancestor from either branch
before recording this merge revision.
"""

from collections.abc import Sequence

revision: str = "0099"
down_revision: tuple[str, str] = ("0098", "0093")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

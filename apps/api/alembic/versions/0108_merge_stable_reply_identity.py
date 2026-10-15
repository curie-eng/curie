"""Join the feature train and stable reply identity migration histories.

Revision ID: 0108
Revises: 0107, 0106
Create Date: 2026-10-13
"""

from collections.abc import Sequence

revision: str = "0108"
down_revision: tuple[str, str] = ("0107", "0106")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

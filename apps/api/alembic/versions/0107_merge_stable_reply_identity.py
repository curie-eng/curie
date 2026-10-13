"""Join the feature train and stable reply identity migration histories.

Revision ID: 0107
Revises: 0105, 0106
Create Date: 2026-10-13
"""

from collections.abc import Sequence

revision: str = "0107"
down_revision: tuple[str, str] = ("0105", "0106")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

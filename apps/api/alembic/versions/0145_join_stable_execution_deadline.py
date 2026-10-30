"""Join the stable execution deadline ceiling and the feature factory base.

Revision ID: 0145
Revises: 0140, 0144
"""

from collections.abc import Sequence

revision: str = "0145"
down_revision: tuple[str, str] = ("0140", "0144")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

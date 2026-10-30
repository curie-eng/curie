"""Join the stable twelve-hour deadline and the feature factory base transitions.

0140 (the v0.14.3 backport) and 0136 (on this line) make the same constraint
replacement, so either upgrade order leaves the 43200 s checks.

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

"""Join stable start diagnostics and the feature attachment catalog.

Revision ID: 0131
Revises: 0123, 0130
"""

from collections.abc import Sequence

revision: str = "0131"
down_revision: tuple[str, str] = ("0123", "0130")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

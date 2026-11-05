"""Join the publication UI evidence tables (0146) and deleted issue cancellation (0147).

0146 only creates new tables and 0147 only rewrites the execution request state
check, so either upgrade order reaches the same schema.

Revision ID: 0149
Revises: 0146, 0147
"""

from collections.abc import Sequence

revision: str = "0149"
down_revision: tuple[str, str] = ("0146", "0147")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

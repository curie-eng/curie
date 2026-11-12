"""Join publication UI evidence and deleted-issue cancellation.

0146 creates the publication UI evidence tables and indexes on next. 0147,
released in v0.15.3, replaces the execution_requests state-shape check
constraint. They touch disjoint objects, so either upgrade order is safe.

Revision ID: 0148
Revises: 0146, 0147
"""

from collections.abc import Sequence

revision: str = "0148"
down_revision: tuple[str, str] = ("0146", "0147")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

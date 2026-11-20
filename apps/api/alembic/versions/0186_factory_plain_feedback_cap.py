"""Count the revisions plain review comments start on each pull request (#5794).

``thread_publication_lineages`` has one row per factory pull request. It gains
``plain_feedback_revisions``, how many revisions feedback without a factory
mention has started there, and ``plain_feedback_capped_at``, when the first
plain comment past the cap was refused and the one mention-needed notice was
posted. Feedback that mentions the factory is never counted.

Expand only: existing rows start at 0 and NULL, and a release without this
revision never reads or writes either column; its inserts take the defaults.
The downgrade drops both columns. A release without them has no cap, so the
counts carry no meaning for it; upgrading again starts every pull request at 0.

Revision ID: 0186
Revises: 0185
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0186"
down_revision: str = "0185"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

S = "curie"
LOCK_TIMEOUT_MS = 15000
TABLE = "thread_publication_lineages"
COUNT_CHECK = "thread_publication_lineages_plain_feedback_revisions_ck"


def upgrade() -> None:
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    op.execute(
        sa.text(
            f"""
            ALTER TABLE {S}.{TABLE}
              ADD COLUMN plain_feedback_revisions integer NOT NULL DEFAULT 0,
              ADD COLUMN plain_feedback_capped_at timestamptz,
              ADD CONSTRAINT {COUNT_CHECK} CHECK (plain_feedback_revisions >= 0)
            """
        )
    )


def downgrade() -> None:
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    op.drop_constraint(COUNT_CHECK, TABLE, schema=S, type_="check")
    op.drop_column(TABLE, "plain_feedback_capped_at", schema=S)
    op.drop_column(TABLE, "plain_feedback_revisions", schema=S)

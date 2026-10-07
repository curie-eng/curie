"""The nomination's execution code (AUTOMATED-REMEDIATION-18)

Revision ID: 0091
Revises: 0090
Create Date: 2026-10-07

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database).

``remediation_nominations`` gains ``execution_code``: the code of a forward
execution that ended ``failed``, ``indeterminate`` or ``refused`` after
admission. Such a nomination gets no verifier and finishes ``not-recovered``
with that code. A refused execution has no ledger row, so the nomination is the
only place the code is kept for reporting. Nullable with no default, so
existing rows read NULL and nothing is backfilled.

The downgrade drops the column.

@spec AUTOMATED-REMEDIATION-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0091"
down_revision: str | None = "0090"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
NOMINATIONS = "remediation_nominations"


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-18."""
    op.add_column(NOMINATIONS, sa.Column("execution_code", sa.Text()), schema=SCHEMA)


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-18."""
    op.drop_column(NOMINATIONS, "execution_code", schema=SCHEMA)

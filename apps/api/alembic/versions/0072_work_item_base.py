"""Record a factory WorkItem's base branch (#3095, ADR 0186).

Adds four nullable text columns to ``work_items``: ``base_branch``,
``base_source`` ('label' or 'default'), ``base_commit`` and
``base_label_ignored``. Branch, source and commit are written together at
admission or not at all; a row with none of them is a legacy WorkItem that keeps
using the repository default branch. ``readmit_base_branch``,
``readmit_base_source`` and ``readmit_base_commit`` carry the base resolved for
a relabel deferred behind a stopping run; it replaces the recorded base when
the replacement request is admitted. Downgrade drops the columns.

Revision ID: 0072
Revises: 0071
Create Date: 2026-10-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0072"
down_revision: str | None = "0071"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
_COLUMNS = (
    "base_branch",
    "base_source",
    "base_commit",
    "base_label_ignored",
    "readmit_base_branch",
    "readmit_base_source",
    "readmit_base_commit",
)


def upgrade() -> None:
    for column in _COLUMNS:
        op.add_column("work_items", sa.Column(column, sa.Text(), nullable=True), schema=SCHEMA)
    op.create_check_constraint(
        "work_items_base_ck",
        "work_items",
        "(base_branch IS NULL AND base_source IS NULL AND base_commit IS NULL) OR "
        "(base_branch IS NOT NULL AND base_source IS NOT NULL AND base_commit IS NOT NULL)",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "work_items_base_source_ck",
        "work_items",
        "base_source IS NULL OR base_source IN ('label', 'default')",
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_constraint("work_items_base_source_ck", "work_items", schema=SCHEMA)
    op.drop_constraint("work_items_base_ck", "work_items", schema=SCHEMA)
    for column in reversed(_COLUMNS):
        op.drop_column("work_items", column, schema=SCHEMA)

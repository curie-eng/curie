"""approval audit admits the adapter principal kind (#2806, ADR-0154)

A channel adapter is a principal: its resolutions record
principal_kind = 'adapter', with the sender it authenticated as the actor and
the adapter itself in the new nullable principal_subject column. NULL is every
other kind and every pre-existing row; no historical record is retro-labelled.

Revision ID: 0045
Revises: 0044
Create Date: 2026-09-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0045"
down_revision: str | None = "0044"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
AUDIT_TABLE = "approval_audit_entries"
PRINCIPAL_KIND_CONSTRAINT = "approval_audit_principal_kind_ck"


def upgrade() -> None:
    op.drop_constraint(PRINCIPAL_KIND_CONSTRAINT, AUDIT_TABLE, type_="check", schema=SCHEMA)
    op.create_check_constraint(
        PRINCIPAL_KIND_CONSTRAINT,
        AUDIT_TABLE,
        "principal_kind IS NULL OR principal_kind IN "
        "('chat', 'console', 'operator', 'adapter')",
        schema=SCHEMA,
    )
    op.add_column(
        AUDIT_TABLE,
        sa.Column("principal_subject", sa.String(), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column(AUDIT_TABLE, "principal_subject", schema=SCHEMA)
    op.drop_constraint(PRINCIPAL_KIND_CONSTRAINT, AUDIT_TABLE, type_="check", schema=SCHEMA)
    # The restored check cannot hold an adapter row; clear the kind rather than
    # deleting audit history, the same honest NULL a pre-0038 row carries.
    op.execute(
        f"UPDATE {SCHEMA}.{AUDIT_TABLE} SET principal_kind = NULL "
        "WHERE principal_kind = 'adapter'"
    )
    op.create_check_constraint(
        PRINCIPAL_KIND_CONSTRAINT,
        AUDIT_TABLE,
        "principal_kind IS NULL OR principal_kind IN ('chat', 'console', 'operator')",
        schema=SCHEMA,
    )

"""Read executions, sample scheduling and the sample (AUTOMATED-REMEDIATION-12)

Revision ID: 0090
Revises: 0089
Create Date: 2026-10-07

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database).

``action_executions`` gains ``not_before`` (executor amendment E9: the claim
route hands out an execution only once it is due; NULL is due, so every
existing row stays claimable exactly as before), ``pointer`` (a read
execution's RFC 6901 pointer, bound by its producer with the tool and
arguments) and ``sample`` (the ``{"sample", "value"}`` one read execution
reported, or the API's ``skipped``). Every new column is nullable with no
default, so existing rows read NULL and nothing is backfilled.

``action_executions_kind_ck`` is replaced in the same revision so ``kind``
gains ``read`` (executor amendment E2). A partial index serves the claim
route's due-first order over requested rows.

The downgrade removes read executions (samples only: a read never dispatches,
so no ledger row names one), restores the kind check without ``read`` and
drops the index and the new columns.

@spec AUTOMATED-REMEDIATION-12
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0090"
down_revision: str | None = "0089"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
EXECUTIONS = "action_executions"
KIND_CHECK = "action_executions_kind_ck"
DUE_INDEX = "ix_action_executions_requested_not_before"

KINDS_WITH_READ = "kind IN ('restore', 'forward', 'probe', 'read')"
KINDS_WITHOUT_READ = "kind IN ('restore', 'forward', 'probe')"


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-12."""
    op.add_column(EXECUTIONS, sa.Column("not_before", sa.DateTime(timezone=True)), schema=SCHEMA)
    op.add_column(EXECUTIONS, sa.Column("pointer", sa.Text()), schema=SCHEMA)
    op.add_column(EXECUTIONS, sa.Column("sample", postgresql.JSONB()), schema=SCHEMA)
    op.drop_constraint(KIND_CHECK, EXECUTIONS, type_="check", schema=SCHEMA)
    op.create_check_constraint(KIND_CHECK, EXECUTIONS, KINDS_WITH_READ, schema=SCHEMA)
    op.create_index(
        DUE_INDEX,
        EXECUTIONS,
        ["not_before", "created_at"],
        schema=SCHEMA,
        postgresql_where=sa.text("state = 'requested'"),
    )


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-12."""
    op.drop_index(DUE_INDEX, table_name=EXECUTIONS, schema=SCHEMA)
    op.execute(f"DELETE FROM {SCHEMA}.{EXECUTIONS} WHERE kind = 'read'")
    op.drop_constraint(KIND_CHECK, EXECUTIONS, type_="check", schema=SCHEMA)
    op.create_check_constraint(KIND_CHECK, EXECUTIONS, KINDS_WITHOUT_READ, schema=SCHEMA)
    for column in ("sample", "pointer", "not_before"):
        op.drop_column(EXECUTIONS, column, schema=SCHEMA)

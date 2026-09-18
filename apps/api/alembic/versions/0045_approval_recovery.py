"""Break-glass approval recovery columns (#2753)

Additive only. ``recovery_key`` is the idempotency key of the administrative
operation that last touched the row (UNIQUE where not null, so a replay is a
read rather than a second act). ``resume_cancelled_*`` is the resume tombstone:
a non-NULL ``resume_cancelled_at`` vetoes execution of an owed wake without
deleting any reply-identity column or any audit row. ``resume_executing_*`` is
the execution RECORD the worker writes at the execution boundary: when it
started and under which delivery lease (key, owner token, fencing generation).
It is not a claim and excludes nothing. A resume with any recorded execution
cannot be cancelled; the lease columns only name the delivery in that refusal.

Takes the identity fence first, like 0022 and 0024: an install already past
0024 runs only this revision, and without the fence its ``ADD COLUMN`` would
wait unbounded behind any long reader of ``approvals``.

Revision ID: 0045
Revises: 0044
Create Date: 2026-09-18
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from curie_api.migration_fence import fence_identity_tables

revision: str = "0045"
down_revision: str | None = "0044"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    # The identity fence, same tables in the same order as 0022 and 0024, taken
    # up front so the ADD COLUMNs below are not a lock upgrade.
    fence_identity_tables(op.get_bind())
    op.add_column("approvals", sa.Column("recovery_key", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column(
        "approvals",
        sa.Column("resume_cancelled_at", sa.DateTime(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "approvals",
        sa.Column("resume_cancelled_reason", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "approvals",
        sa.Column("resume_cancelled_by", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    # The execution RECORD. Written unconditionally (bar the tombstone) by the
    # worker at the execution boundary, so a redelivery after a crash simply
    # re-records. The lease columns name the delivery lease the execution runs
    # under, which the cancellation refusal reports; nothing arbitrates on them.
    op.add_column(
        "approvals",
        sa.Column("resume_executing_at", sa.DateTime(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "approvals",
        sa.Column("resume_executing_lease_key", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "approvals",
        sa.Column("resume_executing_owner", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "approvals",
        sa.Column("resume_executing_generation", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    # Partial unique: every pre-existing row keeps NULL, and two rows can never
    # share one recovery key, which is what makes replay a read of ONE outcome.
    op.create_index(
        "uq_approvals_recovery_key",
        "approvals",
        ["recovery_key"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("recovery_key IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_approvals_recovery_key", table_name="approvals", schema=SCHEMA)
    op.drop_column("approvals", "resume_executing_generation", schema=SCHEMA)
    op.drop_column("approvals", "resume_executing_owner", schema=SCHEMA)
    op.drop_column("approvals", "resume_executing_lease_key", schema=SCHEMA)
    op.drop_column("approvals", "resume_executing_at", schema=SCHEMA)
    op.drop_column("approvals", "resume_cancelled_by", schema=SCHEMA)
    op.drop_column("approvals", "resume_cancelled_reason", schema=SCHEMA)
    op.drop_column("approvals", "resume_cancelled_at", schema=SCHEMA)
    op.drop_column("approvals", "recovery_key", schema=SCHEMA)

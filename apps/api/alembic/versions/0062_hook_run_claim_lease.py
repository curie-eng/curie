"""Lease on a hook run claim and the reclaimed outcome (#2931, ADR-0099).

Adds nullable ``hook_runs.lease_expires_at``. The cron scheduler sets it on
every in-flight claim, and the next fire of the hook reclaims a claim whose
lease has lapsed, closing it ``reclaimed``. Terminal rows carry no lease.

There is no backfill. The scheduler holds a claim with no lease, one made
before this revision or by an older worker during a rollout, for one
configured lease from its start. ``outcome`` gains ``reclaimed``.

Revision ID: 0062
Revises: 0061
Create Date: 2026-09-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0062"
down_revision: str | None = "0061"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.add_column(
        "hook_runs",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.drop_constraint("hook_runs_outcome_ck", "hook_runs", schema=SCHEMA, type_="check")
    op.create_check_constraint(
        "hook_runs_outcome_ck",
        "hook_runs",
        "outcome IS NULL OR outcome IN "
        "('ran', 'deferred', 'skipped', 'blocked', 'reclaimed', 'failed')",
        schema=SCHEMA,
    )


def downgrade() -> None:
    # A reclaimed claim was a failed run under the earlier vocabulary.
    op.execute(f"UPDATE {SCHEMA}.hook_runs SET outcome = 'failed' WHERE outcome = 'reclaimed'")
    op.drop_constraint("hook_runs_outcome_ck", "hook_runs", schema=SCHEMA, type_="check")
    op.create_check_constraint(
        "hook_runs_outcome_ck",
        "hook_runs",
        "outcome IS NULL OR outcome IN ('ran', 'deferred', 'skipped', 'blocked', 'failed')",
        schema=SCHEMA,
    )
    op.drop_column("hook_runs", "lease_expires_at", schema=SCHEMA)

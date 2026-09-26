"""Allow ``deferred`` as a hook run outcome (#2929).

A cron fire whose thread holds a live session is recorded ``deferred`` and
reopened by the scheduler on a later tick (ADR-0099, Concurrency and idle).

Revision ID: 0060
Revises: 0059
Create Date: 2026-09-25
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0060"
down_revision: str | None = "0059"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.drop_constraint("hook_runs_outcome_ck", "hook_runs", schema=SCHEMA, type_="check")
    op.create_check_constraint(
        "hook_runs_outcome_ck",
        "hook_runs",
        "outcome IS NULL OR outcome IN ('ran', 'deferred', 'skipped', 'blocked', 'failed')",
        schema=SCHEMA,
    )


def downgrade() -> None:
    # A deferred row has no pre-0060 spelling; it is a fire that never ran.
    op.execute(f"UPDATE {SCHEMA}.hook_runs SET outcome = 'skipped' WHERE outcome = 'deferred'")
    op.drop_constraint("hook_runs_outcome_ck", "hook_runs", schema=SCHEMA, type_="check")
    op.create_check_constraint(
        "hook_runs_outcome_ck",
        "hook_runs",
        "outcome IS NULL OR outcome IN ('ran', 'skipped', 'blocked', 'failed')",
        schema=SCHEMA,
    )

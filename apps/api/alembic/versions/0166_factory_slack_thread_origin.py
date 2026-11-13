"""Factory Slack thread origin: configured progress roots and kickoff roots (#5380).

``factory_slack_threads.origin`` records why a WorkItem is bound to a Slack
root. ``configured`` is a root the agent's factory progress destination
created (every row that exists before this revision); ``kickoff`` is a root a
person started by mentioning the agent with an issue link, which owes progress
into its own thread whether or not the agent has a destination configured.

The column is ``NOT NULL DEFAULT 'configured'``. The default is kept on
purpose: an application at the prior revision inserts thread rows without
naming ``origin``, and those rows must stay valid. Adding a column with a
constant default is a metadata-only change, so the upgrade does not rewrite
the table.

The downgrade refuses while any ``kickoff`` row exists: dropping the column
would turn those rows into configured threads, and progress would then stop
for any agent without a destination, orphaning the person's thread. It takes
an exclusive lock on the table first, under a bounded wait, so a kickoff row
cannot be inserted between the check and the drop, and it refuses before any
DDL.

Revision ID: 0166
Revises: 0162
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0166"
down_revision: str = "0162"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"
TABLE = "factory_slack_threads"
CHECK = "factory_slack_threads_origin_ck"
ORIGINS = ("configured", "kickoff")

# A bounded wait for the table lock, so a busy table refuses the downgrade
# rather than blocking writers queued behind it indefinitely.
LOCK_TIMEOUT_MS = 15000


def upgrade() -> None:
    op.add_column(
        TABLE,
        sa.Column("origin", sa.Text(), nullable=False, server_default="configured"),
        schema=S,
    )
    op.create_check_constraint(
        CHECK,
        TABLE,
        f"origin IN ({', '.join(repr(origin) for origin in ORIGINS)})",
        schema=S,
    )


def downgrade() -> None:
    # Held from the check to the drop, so no kickoff row lands in between.
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    op.execute(sa.text(f"LOCK TABLE {S}.{TABLE} IN ACCESS EXCLUSIVE MODE"))
    if op.get_bind().scalar(
        sa.text(f"SELECT EXISTS (SELECT 1 FROM {S}.{TABLE} WHERE origin = 'kickoff')")
    ):
        raise RuntimeError("factory_slack_kickoff_threads_present")
    op.drop_constraint(CHECK, TABLE, schema=S)
    op.drop_column(TABLE, "origin", schema=S)

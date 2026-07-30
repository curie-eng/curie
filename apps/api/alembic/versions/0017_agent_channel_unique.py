"""one agent per Slack channel: unique agents.slack_channel (#38)

`agents.slack_channel` was a plain non-unique column, so the API happily created
N agents on one channel -- but the worker's binding resolver maps a channel to a
single agent (`ORDER BY (environment='prod') DESC, deployed_at DESC`), so every
agent after the winner was silently shadowed: deployed, healthy-looking, and
never answering in Slack. Issue #38 asked for a decision between enforcing 1:1
and supporting N-per-channel intentionally; 1:1 is the choice, matching what the
resolver already assumes.

A pre-existing install can already hold duplicates, and creating the constraint
on that data fails with a bare `duplicate key value` naming one channel. Check
first and raise an actionable error listing every offending channel, so the
operator can re-point or delete the shadowed agents and re-run.

Revision ID: 0017
Revises: 0016
Create Date: 2026-07-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0017"
down_revision: str | None = "0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
CONSTRAINT = "agents_slack_channel_key"


def upgrade() -> None:
    conn = op.get_bind()
    duplicates = conn.execute(
        sa.text(
            f"""
            SELECT slack_channel, count(*) AS n, string_agg(name, ', ' ORDER BY name) AS agents
            FROM {SCHEMA}.agents
            GROUP BY slack_channel
            HAVING count(*) > 1
            ORDER BY slack_channel
            """
        )
    ).all()
    if duplicates:
        detail = "; ".join(
            f"{row.slack_channel}: {row.n} agents ({row.agents})" for row in duplicates
        )
        raise RuntimeError(
            "cannot enforce one agent per Slack channel (#38): these channels have "
            f"more than one agent bound -- {detail}. Only one agent per channel can "
            "ever respond (the worker's resolver picks prod-first, then the most "
            "recently deployed, and shadows the rest), so the extras are already "
            "inert. Re-point them to their own channels or delete them, then re-run "
            "this migration."
        )
    op.create_unique_constraint(CONSTRAINT, "agents", ["slack_channel"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_constraint(CONSTRAINT, "agents", type_="unique", schema=SCHEMA)

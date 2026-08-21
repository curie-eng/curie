"""one agent may hold more than one channel binding (ADR-0116, #1459)

Drops `agent_channels_agent_id_key`, the constraint that restricted an agent to
exactly one row in `agent_channels`. ADR-0089's "one agent still binds one
channel" is superseded in part by ADR-0116: `agent_channels` already models a
binding as a row, and the only thing stopping an agent from holding several was
this constraint. The worker resolves `(kind, address)` to an agent
(`binding._RESOLVE_SQL`), never the reverse, so N rows per `agent_id` change
nothing there, and a reply routes from the inbound turn's own `ReplyHandle`,
never from a per-agent lookup.

A PLAIN index on `agent_id` replaces the dropped constraint's backing index.
That index was the only one on the column, and the API's side of this feature
leans on it hard: every binding write locks the agent's set first
(`crud.lock_agent_bindings` filters and orders by `agent_id` with
`FOR UPDATE`), so without it each add, move and delete escalates to a sequential
scan of the whole table -- widening the very lock window this revision makes it
possible to contend for. Non-unique, because holding several bindings is the
entire point of the revision.

**`agent_channels_kind_address_key` (0023) is deliberately NOT touched.** A
`(kind, address)` pair still identifies at most one binding -- two agents still
cannot claim the same channel, which is the ambiguity #38 exists to prevent.
This revision only widens the agent-side constraint.

`downgrade` PRE-FLIGHTS and refuses BY NAME, following 0023's discipline:
once an agent holds two or more bindings there is no honest single row to
collapse back onto, and a bare `duplicate key value violates unique
constraint` names only one of the offending rows and hides which agent and
which pairs they belong to. The message lists the agent id, the kind(s) it
holds, and the row count per agent instead -- never the bound addresses,
which would otherwise land in deployment logs.

Revision ID: 0028
Revises: 0027
Create Date: 2026-08-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0028"
down_revision: str | None = "0027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "agent_channels"

# Spelled out, following 0023's discipline: the constraint the API's 409 map
# keys on by literal name, and the one this revision drops.
OLD_CONSTRAINT = "agent_channels_agent_id_key"
# NOT touched by this migration: two agents still cannot share one (kind,
# address) pair (0023).
KIND_ADDRESS_CONSTRAINT = "agent_channels_kind_address_key"
# The plain replacement for the dropped constraint's backing index, named on
# `ix_<table>_<column>` (0018's convention) so `models.py` can declare the same
# identity and autogenerate stays quiet.
AGENT_ID_INDEX = "ix_agent_channels_agent_id"


def upgrade() -> None:
    # Created BEFORE the drop so the column is never left unindexed, not even
    # for the width of this migration.
    op.create_index(AGENT_ID_INDEX, TABLE, ["agent_id"], unique=False, schema=SCHEMA)
    op.drop_constraint(OLD_CONSTRAINT, TABLE, type_="unique", schema=SCHEMA)


def downgrade() -> None:
    conn = op.get_bind()

    # Names the agent, its kind(s), and its row count -- never the bound
    # addresses, which would otherwise land in deployment logs (security).
    offenders = conn.execute(
        sa.text(
            f"""
            SELECT agent_id,
                   string_agg(DISTINCT kind, ', ' ORDER BY kind) AS kinds,
                   count(*) AS row_count
            FROM {SCHEMA}.{TABLE}
            GROUP BY agent_id
            HAVING count(*) > 1
            ORDER BY agent_id
            """
        )
    ).all()
    if offenders:
        detail = "; ".join(
            f"{row.agent_id} (kind: {row.kinds}, rows: {row.row_count})" for row in offenders
        )
        raise RuntimeError(
            "cannot restore one-binding-per-agent (ADR-0116, #1459): these "
            f"agents hold more than one channel binding -- {detail}. There is "
            "no honest way to collapse an agent's bindings back onto a single "
            "row -- one of them would have to be discarded. Move or delete all "
            "but one binding per agent, then re-run this downgrade."
        )

    # Dropped BEFORE the constraint goes back: the restored unique constraint
    # brings its own backing index on the same column, and leaving both would
    # hand the pre-0028 schema a duplicate index it never had.
    op.drop_index(AGENT_ID_INDEX, table_name=TABLE, schema=SCHEMA)
    op.create_unique_constraint(OLD_CONSTRAINT, TABLE, ["agent_id"], schema=SCHEMA)

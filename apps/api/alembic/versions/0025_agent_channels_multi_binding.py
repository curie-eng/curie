"""one agent may hold more than one channel binding (ADR-0107, #1459)

Drops `agent_channels_agent_id_key`, the constraint that restricted an agent to
exactly one row in `agent_channels`. ADR-0089's "one agent still binds one
channel" is superseded in part by ADR-0107: `agent_channels` already models a
binding as a row, and the only thing stopping an agent from holding several was
this constraint. Nothing else about the table changes -- the worker resolves
`(kind, address)` to an agent (`binding._RESOLVE_SQL`), never the reverse, so N
rows per `agent_id` change nothing there, and a reply routes from the inbound
turn's own `ReplyHandle`, never from a per-agent lookup.

**`agent_channels_kind_address_key` (0023) is deliberately NOT touched.** A
`(kind, address)` pair still identifies at most one binding -- two agents still
cannot claim the same channel, which is the ambiguity #38 exists to prevent.
This revision only widens the agent-side constraint.

`downgrade` PRE-FLIGHTS and refuses BY NAME, following 0023's discipline:
once an agent holds two or more bindings there is no honest single row to
collapse back onto, and a bare `duplicate key value violates unique
constraint` names only one of the offending rows and hides which agent and
which pairs they belong to. The message lists the agent id and every one of
its bound addresses instead.

Revision ID: 0025
Revises: 0024
Create Date: 2026-08-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0025"
down_revision: str | None = "0024"
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


def upgrade() -> None:
    op.drop_constraint(OLD_CONSTRAINT, TABLE, type_="unique", schema=SCHEMA)


def downgrade() -> None:
    conn = op.get_bind()

    offenders = conn.execute(
        sa.text(
            f"""
            SELECT agent_id, string_agg(address, ', ' ORDER BY address) AS addresses
            FROM {SCHEMA}.{TABLE}
            GROUP BY agent_id
            HAVING count(*) > 1
            ORDER BY agent_id
            """
        )
    ).all()
    if offenders:
        detail = "; ".join(f"{row.agent_id} (addresses: {row.addresses})" for row in offenders)
        raise RuntimeError(
            "cannot restore one-binding-per-agent (ADR-0107, #1459): these "
            f"agents hold more than one channel binding -- {detail}. There is "
            "no honest way to collapse an agent's bindings back onto a single "
            "row -- one of them would have to be discarded. Move or delete all "
            "but one binding per agent, then re-run this downgrade."
        )

    op.create_unique_constraint(OLD_CONSTRAINT, TABLE, ["agent_id"], schema=SCHEMA)

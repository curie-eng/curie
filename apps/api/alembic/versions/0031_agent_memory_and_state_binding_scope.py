"""agents.memory and workflow_state_entries.binding_scope (#1525 follow-up)

ADR-0118 decision 2 makes binding CARDINALITY the whole opt-in: adding a second
`agent_channels` row is what turns an agent multi-surface, with no separate
flag. That is correct for ROUTING and for the agent-scoped controls (budget,
kill state, bundle version, approval policy) -- those already apply
uniformly to every binding regardless of count, unconditionally, and this
migration changes nothing about them.

It is the wrong default for workflow state specifically. Curie cannot observe
or refuse a Slack invite (the dispatcher subscribes to `app_mention` and
`message.im` only; nothing in the manifest asks for `member_joined_channel` or
a `conversations.list`-shaped scope), and a channel the bot is a member of
still reaches the worker once an operator explicitly binds it -- there being
no signal to gate on is exactly why binding creation itself stays
unconditional. But an operator who explicitly binds a second channel for
routing reasons (a dev/prod split of the SAME channel-neutral pair, a second
support queue) has not necessarily decided that channel should read and write
the first channel's cross-turn business state. Cardinality alone cannot
distinguish "route this too" from "share memory with that."

`agents.memory` makes the second question explicit. Defaults to False, so
every agent that exists today keeps exactly the behavior it already has
(one binding, nothing to share with) and a newly bound second channel starts
isolated rather than silently inheriting whatever the first channel already
wrote. `workflow_state_entries.binding_scope` is NULL for a memory=True
agent's one shared row, or `"{kind}:{address}"` per binding for a memory=False
agent -- selected by the worker at claim time from the agent's CURRENT
`memory` value when it mints the turn's `state`/`state.app` token, and the API
never reads `agents.memory` itself; it trusts whatever scope the signed token
names. Part of the unique key, not just a filter column, so a memory=False
agent's second binding gets its OWN row for a given namespace+key instead of
colliding with (or silently reading) the first binding's.

Revision ID: 0031
Revises: 0030
Create Date: 2026-08-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0031"
down_revision: str | None = "0030"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
STATE_TABLE = "workflow_state_entries"
OLD_STATE_CONSTRAINT = "uq_state_agent_ns_key"
NEW_STATE_CONSTRAINT = "uq_state_agent_scope_ns_key"


def upgrade() -> None:
    # NOT NULL with a server default, same discipline as 0027's hook_generation:
    # the backfill and the constraint land in one statement, so no window exists
    # where an existing agent reads as NULL and every existing agent starts
    # exactly where it already was (one binding, memory moot either way).
    op.add_column(
        "agents",
        sa.Column("memory", sa.Boolean(), server_default=sa.false(), nullable=False),
        schema=SCHEMA,
    )
    # Nullable: every existing row backfills to NULL, which is the memory=True
    # shape (one shared row) -- the correct read of history, since every row
    # written before this column existed was written under the old, unqualified
    # agent_id-only key that this migration is narrowing.
    op.add_column(
        "workflow_state_entries",
        sa.Column("binding_scope", sa.String(), nullable=True),
        schema=SCHEMA,
    )
    # Widened, not narrowed: the old constraint's rows (all binding_scope=NULL)
    # still satisfy the new one, since Postgres treats NULL as distinct across
    # rows in a unique index rather than colliding on it. Dropped and recreated
    # rather than altered because Postgres has no ALTER CONSTRAINT for adding a
    # column to one.
    op.drop_constraint(OLD_STATE_CONSTRAINT, STATE_TABLE, type_="unique", schema=SCHEMA)
    op.create_unique_constraint(
        NEW_STATE_CONSTRAINT,
        STATE_TABLE,
        ["agent_id", "binding_scope", "namespace", "key"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    conn = op.get_bind()

    # A memory=False agent's per-binding rows have no honest way to collapse
    # onto the old agent_id-only key: two rows can genuinely share a
    # namespace+key today (one per binding) and the pre-0031 constraint allowed
    # only one. Preflight and refuse by name, following 0030's downgrade
    # discipline -- a bare `duplicate key value violates unique constraint`
    # would name one colliding row and hide which agent and which scopes it
    # belongs to.
    offenders = conn.execute(
        sa.text(
            f"""
            SELECT agent_id, namespace, key, count(*) AS row_count
            FROM {SCHEMA}.{STATE_TABLE}
            WHERE binding_scope IS NOT NULL
            GROUP BY agent_id, namespace, key
            ORDER BY agent_id, namespace, key
            """
        )
    ).all()
    if offenders:
        detail = "; ".join(
            f"{row.agent_id} {row.namespace}/{row.key} ({row.row_count} scoped rows)"
            for row in offenders
        )
        raise RuntimeError(
            "cannot restore the agent_id-only state key (#1525 follow-up): these "
            f"agents hold per-binding state rows -- {detail}. There is no honest "
            "way to collapse per-binding rows back onto one row -- some of them "
            "would have to be discarded. Set every affected agent back to "
            "memory=true and merge or delete the extra rows, then re-run this "
            "downgrade."
        )

    op.drop_constraint(NEW_STATE_CONSTRAINT, STATE_TABLE, type_="unique", schema=SCHEMA)
    op.create_unique_constraint(
        OLD_STATE_CONSTRAINT, STATE_TABLE, ["agent_id", "namespace", "key"], schema=SCHEMA
    )
    op.drop_column("workflow_state_entries", "binding_scope", schema=SCHEMA)
    op.drop_column("agents", "memory", schema=SCHEMA)

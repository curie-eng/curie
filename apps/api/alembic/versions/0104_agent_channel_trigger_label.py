"""A tracker surface carries an optional trigger label (#4361, #4397 decision 10).

Two factory agents may serve one tracker location when their surfaces carry
different trigger labels. ``agent_channels`` gains a nullable
``trigger_label`` (``String(50)``), allowed only on a tracker surface kind
(``github``, ``gitlab``, ``jira``) by ``agent_channels_trigger_label_ck``.
``agent_channels_route_key`` widens from ``(kind, address, adapter)`` to
``(kind, address, adapter, trigger_label)``, still NULLS NOT DISTINCT, so two
unlabelled rows on one route keep colliding. A NULL label means the configured
tracker label; the API compares effective labels under the pair advisory lock,
which the index cannot see.

``work_items`` gains a nullable ``trigger_label`` (``String(50)``): the label
that admitted the WorkItem, which the cancellation path compares a removed
label with. Rows admitted before this revision keep NULL; there is no backfill,
because the tracker label lives in the API's forge config, not in the migrate
Job's environment.

Downgrade refuses, naming each location, while any ``(kind, address,
adapter)`` holds two rows, since the narrower key cannot hold them. Otherwise
it restores the old key and drops the CHECK and both columns.

Revision ID: 0104
Revises: 0103
Create Date: 2026-10-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0104"
down_revision: str | None = "0103"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "agent_channels"
WORK_ITEMS = "work_items"
COLUMN = "trigger_label"
ROUTE_KEY = "agent_channels_route_key"
LABEL_CHECK = "agent_channels_trigger_label_ck"
# Frozen copy of models.TRIGGER_LABEL_CHECK at this revision.
LABEL_RULE = "trigger_label IS NULL OR kind IN ('github', 'gitlab', 'jira')"
MAX_LABEL_LENGTH = 50


def upgrade() -> None:
    op.add_column(
        TABLE,
        sa.Column(COLUMN, sa.String(MAX_LABEL_LENGTH), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        WORK_ITEMS,
        sa.Column(COLUMN, sa.String(MAX_LABEL_LENGTH), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(LABEL_CHECK, TABLE, LABEL_RULE, schema=SCHEMA)
    op.drop_constraint(ROUTE_KEY, TABLE, type_="unique", schema=SCHEMA)
    op.create_unique_constraint(
        ROUTE_KEY,
        TABLE,
        ["kind", "address", "adapter", COLUMN],
        schema=SCHEMA,
        postgresql_nulls_not_distinct=True,
    )


def downgrade() -> None:
    conn = op.get_bind()
    shared = conn.execute(
        sa.text(
            "SELECT c.kind, c.address, string_agg(a.name || COALESCE(' [label ' "
            "|| c.trigger_label || ']', ''), ', ' ORDER BY a.name) "
            f"FROM {SCHEMA}.{TABLE} c JOIN {SCHEMA}.agents a ON a.id = c.agent_id "
            "GROUP BY c.kind, c.address, c.adapter HAVING count(*) > 1 "
            "ORDER BY c.kind, c.address"
        )
    ).all()
    if shared:
        rows = "; ".join(f"{kind}:{address} held by {held}" for kind, address, held in shared)
        raise RuntimeError(
            "cannot downgrade below 0104: several bindings share one route under different "
            f"trigger labels, which the pre-0104 key holds to one row ({rows}). Remove all "
            "but one binding on each first. Nothing was changed."
        )
    op.drop_constraint(ROUTE_KEY, TABLE, type_="unique", schema=SCHEMA)
    op.create_unique_constraint(
        ROUTE_KEY,
        TABLE,
        ["kind", "address", "adapter"],
        schema=SCHEMA,
        postgresql_nulls_not_distinct=True,
    )
    op.drop_constraint(LABEL_CHECK, TABLE, type_="check", schema=SCHEMA)
    op.drop_column(WORK_ITEMS, COLUMN, schema=SCHEMA)
    op.drop_column(TABLE, COLUMN, schema=SCHEMA)

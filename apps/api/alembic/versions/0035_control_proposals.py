"""control_proposals: fleet actions the control agent proposes and a human runs

ADR-0133. The control agent reads the fleet with a scoped ``control`` token and
writes rows here. It has no execute path: the execute route accepts the platform
key only, so this table is the boundary between what the model may compute and
what a human may run.

The row stores an API-rendered ``summary`` rather than proposer text, because the
human's click is the authorization and the model must not author the sentence
that click is based on.

Revision ID: 0035
Revises: 0034
Create Date: 2026-08-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0035"
down_revision: str | None = "0034"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "control_proposals",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        # SET NULL rather than CASCADE: deleting an agent must not erase the
        # record of its own deletion. See the model docstring.
        sa.Column(
            "target_agent_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.agents.id", ondelete="SET NULL"),
            nullable=True,
        ),
        # The name as it was, so a row whose agent is gone still reads.
        sa.Column("target_agent_name", sa.String(), nullable=False),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column(
            "params",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("summary", sa.String(), nullable=False),
        # SET NULL rather than CASCADE: deleting the control agent must not
        # erase the record of what it proposed. The audit trail outlives the
        # proposer.
        sa.Column(
            "proposed_by_agent_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.agents.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("requested_by", sa.String(), nullable=True),
        sa.Column("thread_key", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("executed_at", sa.DateTime(), nullable=True),
        sa.Column("executed_by", sa.String(), nullable=True),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_control_proposals_target_agent_id",
        "control_proposals",
        ["target_agent_id"],
        schema=SCHEMA,
    )
    op.create_index(
        "ix_control_proposals_proposed_by_agent_id",
        "control_proposals",
        ["proposed_by_agent_id"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_control_proposals_proposed_by_agent_id",
        table_name="control_proposals",
        schema=SCHEMA,
    )
    op.drop_index(
        "ix_control_proposals_target_agent_id",
        table_name="control_proposals",
        schema=SCHEMA,
    )
    op.drop_table("control_proposals", schema=SCHEMA)

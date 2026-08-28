"""delegation_calls, delegate_grants: ADR-0115 agent-to-agent delegate calls

``delegation_calls`` is the durable record of one agent-to-agent call, carrying
the caller's reply route verbatim (mirrors ``approvals.reply_kind`` etc.) so the
eventual answer can be routed back without re-resolving a binding that may have
moved, plus the call's provenance (``immediate_caller``/``accountable_principal``/
``chain``/``depth``, mirroring ``aci_protocol.DelegationMeta``). ``delegate_grants``
is the operator-armed allowlist (default closed): a call is refused unless a row
here has ``armed=true`` -- the bundle-declared half of that split
(``PluginManifest.delegatesTo``) is validated at deploy, not persisted here.

Revision ID: 0028
Revises: 0027
Create Date: 2026-08-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "0028"
down_revision: str | None = "0027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "delegation_calls",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "caller_agent_id",
            UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("caller_conversation_id", sa.String(), nullable=False),
        sa.Column("caller_reply_kind", sa.String(), nullable=False),
        sa.Column("caller_reply_channel", sa.String(), nullable=False),
        sa.Column("caller_reply_endpoint", sa.String(), nullable=True),
        sa.Column("caller_reply_adapter", sa.String(), nullable=True),
        sa.Column(
            "target_agent_id",
            UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("request_text", sa.String(), nullable=False),
        sa.Column("result_text", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        # Mirror ``aci_protocol.DelegationMeta`` (ADR-0115 part 4/6): the
        # durable twin of the wire payload, same relationship the
        # ``caller_reply_*`` columns above have to ``ReplyHandle``.
        sa.Column("immediate_caller", sa.String(), nullable=False),
        sa.Column("accountable_principal", sa.String(), nullable=False),
        sa.Column("chain", JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("depth", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.Column("resolved_at", sa.DateTime(), nullable=True),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_delegation_calls_caller_agent_id",
        "delegation_calls",
        ["caller_agent_id"],
        schema=SCHEMA,
    )
    op.create_index(
        "ix_delegation_calls_target_agent_id",
        "delegation_calls",
        ["target_agent_id"],
        schema=SCHEMA,
    )

    op.create_table(
        "delegate_grants",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "caller_agent_id",
            UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "target_agent_id",
            UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("armed", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("caller_agent_id", "target_agent_id", name="delegate_grants_pair_key"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_delegate_grants_caller_agent_id",
        "delegate_grants",
        ["caller_agent_id"],
        schema=SCHEMA,
    )
    op.create_index(
        "ix_delegate_grants_target_agent_id",
        "delegate_grants",
        ["target_agent_id"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("delegate_grants", schema=SCHEMA)
    op.drop_table("delegation_calls", schema=SCHEMA)

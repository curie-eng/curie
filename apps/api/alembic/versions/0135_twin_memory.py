"""Twin memory: the agent-wide learning toggle and immutable memory revisions.

ADR 0249. Adds ``agents.agent_wide_learning`` (default false) and the
``memory_revisions`` table, then enriches every valid canonical fact already
stored (``memory`` / ``fact-<32 hex>`` whose value is an object with a string
statement) to the typed shape: ``kind="fact"``, ``archived=false``, a title from
the statement's first line (at most 80 characters) and an unknown
``conversation``. Each enriched fact gets one ``baseline`` revision at its
current head version, so versions never reset. No author, conversation or
timestamp is invented: the baseline's actor is the stored author or
``<unknown>``. A malformed entry is left exactly as it was.

Revision ID: selfimp_memory_0001
Revises: selfimp_sched_0001
Create Date: 2026-10-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "selfimp_memory_0001"
down_revision: str | None = "selfimp_sched_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
# The agent-wide scope in memory_revisions.scope (memory_records.AGENT_SCOPE).
# A binding scope is always "<kind>:<address>", so it cannot collide.
AGENT_SCOPE = "agent"

_VALID_FACT = (
    "namespace = 'memory' "
    "AND key ~ '^fact-[0-9a-f]{32}$' "
    "AND jsonb_typeof(value) = 'object' "
    "AND jsonb_typeof(value -> 'statement') = 'string'"
)


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column(
            "agent_wide_learning",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "memory_revisions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("fact_id", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.Column("snapshot", postgresql.JSONB(), nullable=True),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("conversation", sa.Text(), nullable=True),
        sa.Column("reverted_to_version", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "agent_id", "scope", "fact_id", "version", name="uq_memory_revisions_version"
        ),
        schema=SCHEMA,
    )
    op.execute(
        f"""
        UPDATE {SCHEMA}.workflow_state_entries
        SET value = value || jsonb_build_object(
            'kind', 'fact',
            'archived', false,
            'title', left(split_part(value ->> 'statement', E'\\n', 1), 80),
            'conversation', NULL
        )
        WHERE {_VALID_FACT}
        """
    )
    op.execute(
        f"""
        INSERT INTO {SCHEMA}.memory_revisions
            (agent_id, scope, fact_id, version, operation, snapshot, actor, source,
             conversation, reverted_to_version)
        SELECT
            agent_id,
            COALESCE(binding_scope, '{AGENT_SCOPE}'),
            key,
            version,
            'baseline',
            value,
            CASE
                WHEN jsonb_typeof(value -> 'author') = 'string' THEN value ->> 'author'
                ELSE '<unknown>'
            END,
            'migration',
            NULL,
            NULL
        FROM {SCHEMA}.workflow_state_entries
        WHERE {_VALID_FACT}
        """
    )


def downgrade() -> None:
    op.drop_table("memory_revisions", schema=SCHEMA)
    op.drop_column("agents", "agent_wide_learning", schema=SCHEMA)

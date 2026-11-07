"""Record webhook provenance and a wake-key generation.

The source check gains webhook and keeps schedule and manual. No backfill.
hook_wake_keys stores only a generation for an agent hook. A downgrade that
would drop a webhook run or a wake key is refused before any drop.

Revision ID: 0149
Revises: 0148
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0149"
down_revision: str = "0148"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
SOURCE_CHECK = "hook_runs_source_ck"
WAKE_KEYS = "hook_wake_keys"
PRIOR_SOURCES = "source IN ('schedule', 'manual')"
WEBHOOK_SOURCES = "source IN ('schedule', 'manual', 'webhook')"


def _source_check_present() -> bool:
    present = op.get_bind().scalar(
        sa.text(
            "SELECT 1 FROM pg_constraint "
            "WHERE conrelid = 'curie.hook_runs'::regclass "
            "AND conname = 'hook_runs_source_ck'"
        )
    )
    return present is not None


def _count(statement: str) -> int:
    counted = op.get_bind().scalar(sa.text(statement))
    if counted is None:
        raise RuntimeError("hook wake downgrade count is unavailable")
    return int(counted)


def _replace_source_check(expression: str) -> None:
    op.drop_constraint(SOURCE_CHECK, "hook_runs", schema=SCHEMA, type_="check")
    op.create_check_constraint(SOURCE_CHECK, "hook_runs", expression, schema=SCHEMA)


def upgrade() -> None:
    if not _source_check_present():
        raise RuntimeError("hook run source check is unavailable")
    _replace_source_check(WEBHOOK_SOURCES)
    op.create_table(
        WAKE_KEYS,
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("hook", sa.String(63), nullable=False),
        sa.Column(
            "generation",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("agent_id", "hook"),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.CheckConstraint("generation >= 1", name="hook_wake_keys_generation_ck"),
        schema=SCHEMA,
    )


def downgrade() -> None:
    if not _source_check_present():
        raise RuntimeError("hook run source check is unavailable")
    webhooks = _count(f"SELECT count(*) FROM {SCHEMA}.hook_runs WHERE source = 'webhook'")
    keys = _count(f"SELECT count(*) FROM {SCHEMA}.{WAKE_KEYS}")
    if webhooks > 0 or keys > 0:
        raise RuntimeError("cannot downgrade while webhook hook runs or wake keys exist")
    op.drop_table(WAKE_KEYS, schema=SCHEMA)
    _replace_source_check(PRIOR_SOURCES)

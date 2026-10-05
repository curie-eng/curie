"""@spec PROTECTED-HOOK-SOURCE-10.

Revision ID: 0076
Revises: 0075
Create Date: 2026-10-03
"""

import hashlib
import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0076"
down_revision: str | None = "0075"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "hook_source_operations"


def upgrade() -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    op.create_table(
        TABLE,
        sa.Column("agent_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("hook", sa.String(63), primary_key=True),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("intent_sha256", sa.CHAR(64), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column(
            "attempted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["agent_id"], [f"{SCHEMA}.agents.id"], ondelete="CASCADE"),
        sa.CheckConstraint("generation > 0", name="hook_source_operations_generation_ck"),
        sa.CheckConstraint(
            "status IN ('pending', 'committed')", name="hook_source_operations_status_ck"
        ),
        sa.CheckConstraint(
            "intent_sha256 ~ '^[0-9a-f]{64}$'", name="hook_source_operations_intent_ck"
        ),
        sa.UniqueConstraint(
            "agent_id", "hook", "generation", name="uq_hook_source_operation_generation"
        ),
        schema=SCHEMA,
    )
    connection = op.get_bind()
    for row in connection.execute(
        sa.text(f"SELECT * FROM {SCHEMA}.hook_source_policies")
    ).mappings():
        intent = {
            key: row[key]
            for key in ("mode", "tool_access", "runtime_id", "qualification_id", "bundle_digest")
        }
        fingerprint = hashlib.sha256(
            json.dumps(
                intent,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
        ).hexdigest()
        connection.execute(
            sa.text(
                f"INSERT INTO {SCHEMA}.{TABLE} "
                "(agent_id, hook, operation_id, intent_sha256, status, generation, attempted_at) "
                "VALUES (:agent_id, :hook, :operation_id, :intent_sha256, 'committed', "
                ":generation, :attempted_at)"
            ),
            {
                "agent_id": row["agent_id"],
                "hook": row["hook"],
                "operation_id": row["operation_id"],
                "intent_sha256": fingerprint,
                "generation": row["generation"],
                "attempted_at": row["updated_at"],
            },
        )
    op.execute("""
        -- @spec PROTECTED-HOOK-SOURCE-10
        CREATE FUNCTION curie.enforce_hook_source_operations_invariants()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF EXISTS (SELECT 1 FROM curie.agents WHERE id = OLD.agent_id) THEN
                    RAISE EXCEPTION USING ERRCODE = '23514',
                        MESSAGE = 'source operation history cannot be deleted';
                END IF;
                RETURN OLD;
            END IF;
            IF NEW.agent_id IS DISTINCT FROM OLD.agent_id
               OR NEW.hook IS DISTINCT FROM OLD.hook
               OR NEW.operation_id IS DISTINCT FROM OLD.operation_id
               OR NEW.intent_sha256 IS DISTINCT FROM OLD.intent_sha256
               OR NEW.generation IS DISTINCT FROM OLD.generation
               OR NEW.attempted_at IS DISTINCT FROM OLD.attempted_at
               OR OLD.status <> 'pending' OR NEW.status <> 'committed' THEN
                RAISE EXCEPTION USING ERRCODE = '23514',
                    MESSAGE = 'source operation transition is invalid';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        -- @spec PROTECTED-HOOK-SOURCE-10
        CREATE TRIGGER hook_source_operations_invariants
        BEFORE UPDATE OR DELETE ON curie.hook_source_operations
        FOR EACH ROW EXECUTE FUNCTION curie.enforce_hook_source_operations_invariants()
    """)


def downgrade() -> None:
    """@spec PROTECTED-HOOK-SOURCE-10."""
    op.execute("DROP TRIGGER hook_source_operations_invariants ON curie.hook_source_operations")
    op.execute("DROP FUNCTION curie.enforce_hook_source_operations_invariants()")
    op.drop_table(TABLE, schema=SCHEMA)

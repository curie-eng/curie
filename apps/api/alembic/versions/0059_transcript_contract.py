"""Reconcile and remove legacy transcript state rows (#3088).

Revision 0053 copied transcript history while older API instances could still
write the legacy state store. Copy any legacy row that is newer than its table
copy, then remove all legacy transcript rows. The table is authoritative after
this contract migration. Other state namespaces are untouched.

Downgrade restores legacy rows from the table for the preceding API version.

Revision ID: 0059
Revises: 0058
Create Date: 2026-09-26
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0059"
down_revision: str | None = "0058"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
CONSTRAINT = "ck_workflow_state_entries_no_transcript"


def upgrade() -> None:
    # Hold the lock through commit so older API writers cannot add a legacy row
    # between reconciliation and the constraint that rejects future writes.
    op.execute(
        f"LOCK TABLE {SCHEMA}.workflow_state_entries IN SHARE ROW EXCLUSIVE MODE"
    )
    op.execute(
        f"""
        INSERT INTO {SCHEMA}.thread_transcripts
            (id, agent_id, binding_scope, thread_key, value, version, expires_at,
             created_at, updated_at)
        SELECT gen_random_uuid(), agent_id, binding_scope, key, value, version,
               now() + interval '30 days', created_at, updated_at
        FROM {SCHEMA}.workflow_state_entries
        WHERE namespace = 'transcript'
        ON CONFLICT ON CONSTRAINT uq_thread_transcripts_agent_scope_thread
        DO UPDATE SET value = EXCLUDED.value,
                      version = GREATEST(thread_transcripts.version, EXCLUDED.version) + 1,
                      expires_at = EXCLUDED.expires_at,
                      updated_at = EXCLUDED.updated_at
        WHERE EXCLUDED.updated_at > thread_transcripts.updated_at
        """
    )
    op.execute(
        f"DELETE FROM {SCHEMA}.workflow_state_entries WHERE namespace = 'transcript'"
    )
    op.execute(
        f"ALTER TABLE {SCHEMA}.workflow_state_entries "
        f"ADD CONSTRAINT {CONSTRAINT} CHECK (namespace <> 'transcript')"
    )


def downgrade() -> None:
    op.drop_constraint(CONSTRAINT, "workflow_state_entries", schema=SCHEMA, type_="check")
    op.execute(
        f"""
        INSERT INTO {SCHEMA}.workflow_state_entries
            (id, agent_id, binding_scope, namespace, key, value, version,
             created_at, updated_at)
        SELECT gen_random_uuid(), agent_id, binding_scope, 'transcript', thread_key,
               value, version, created_at, updated_at
        FROM {SCHEMA}.thread_transcripts
        ON CONFLICT ON CONSTRAINT uq_state_agent_scope_ns_key
        DO UPDATE SET value = EXCLUDED.value, version = EXCLUDED.version,
                      updated_at = EXCLUDED.updated_at
        WHERE EXCLUDED.updated_at > workflow_state_entries.updated_at
        """
    )

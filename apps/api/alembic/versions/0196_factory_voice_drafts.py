"""Durable recognition, editable feedback and scoped confirmation receipts.

Revision ID: 0196
Revises: 0189
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0196"
down_revision: str = "0189"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '15000ms'")
    op.execute("""
        CREATE TABLE curie.factory_voice_drafts (
            id uuid PRIMARY KEY,
            source_key varchar(64) NOT NULL UNIQUE,
            code_prefix varchar(12) NOT NULL UNIQUE,
            tenant_hash varchar(64) NOT NULL,
            actor_hash varchar(64) NOT NULL,
            audience_hash varchar(64) NOT NULL,
            surface text NOT NULL,
            source jsonb,
            media_sha256 varchar(64),
            transcript text,
            version integer NOT NULL,
            status text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            expires_at timestamptz NOT NULL,
            lease_id uuid,
            lease_until timestamptz,
            retry_at timestamptz,
            admission_id uuid,
            disposition text,
            refusal text,
            CONSTRAINT factory_voice_receipt_ck CHECK (
                (admission_id IS NOT NULL) = (status = 'confirmed')
                AND (disposition IS NOT NULL) = (admission_id IS NOT NULL)
                AND disposition IN ('queued', 'applied')
            ),
            CONSTRAINT factory_voice_terminal_ck CHECK (
                status IN ('recognizing', 'delivering', 'ready')
                OR (source IS NULL AND transcript IS NULL AND media_sha256 IS NULL)
            )
        )
    """)
    op.execute("""
        CREATE INDEX ix_factory_voice_expiry ON curie.factory_voice_drafts (expires_at)
        WHERE source IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX ix_factory_voice_claim
        ON curie.factory_voice_drafts (surface, retry_at, created_at)
        WHERE status IN ('recognizing', 'delivering')
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '15000ms'")
    op.execute("LOCK TABLE curie.factory_voice_drafts IN ACCESS EXCLUSIVE MODE")
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT FROM curie.factory_voice_drafts) THEN
                RAISE EXCEPTION 'factory_voice_drafts_present';
            END IF;
        END $$
    """)
    op.drop_table("factory_voice_drafts", schema="curie")

"""console_sessions: the store behind revocable console sessions (#1044)

ADR-0083. The console holds the shared platform key in browser code today, which
#630 tracks as a release blocker. This table is the durable half of the
replacement: a login code the CLI mints, exchanged once for a session token, both
stored only as SHA-256 hashes so reading this table cannot replay a session, and
revocation expressed as a column (``revoked_at``) rather than as waiting out a
self-contained token's expiry.

Nothing reads this table yet. Slice 1 lands the store and the two exchange
endpoints wired to nothing; ``require_api_key`` starts accepting a session in
slice 2 (#1045).

Revision ID: 0018
Revises: 0017
Create Date: 2026-07-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0018"
down_revision: str | None = "0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    op.create_table(
        "console_sessions",
        sa.Column(
            "id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        # Hashes, never the credential itself.
        sa.Column("login_code_hash", sa.String(), nullable=False),
        sa.Column("login_code_expires_at", sa.DateTime(), nullable=False),
        # NULL until the code is exchanged.
        sa.Column("session_token_hash", sa.String(), nullable=True),
        sa.Column("session_expires_at", sa.DateTime(), nullable=True),
        sa.Column("consumed_at", sa.DateTime(), nullable=True),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.func.now(),
            nullable=False,
        ),
        schema=SCHEMA,
    )
    # Unique so no two rows can satisfy one credential; indexed because both are
    # looked up by hash on every exchange and (from slice 2) every session-authed
    # request.
    op.create_index(
        "ix_console_sessions_login_code_hash",
        "console_sessions",
        ["login_code_hash"],
        unique=True,
        schema=SCHEMA,
    )
    op.create_index(
        "ix_console_sessions_session_token_hash",
        "console_sessions",
        ["session_token_hash"],
        unique=True,
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_console_sessions_session_token_hash",
        table_name="console_sessions",
        schema=SCHEMA,
    )
    op.drop_index(
        "ix_console_sessions_login_code_hash",
        table_name="console_sessions",
        schema=SCHEMA,
    )
    op.drop_table("console_sessions", schema=SCHEMA)

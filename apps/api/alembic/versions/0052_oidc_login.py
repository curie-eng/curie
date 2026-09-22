"""Generic OIDC login: issuer-scoped principals, principal sessions, login attempts.

#2908, ADR 0155 step 3. Three additive pieces:

- ``principals.idp_issuer`` joins the identity key. An OIDC ``sub`` is unique
  only per issuer, so ``(tenant_id, idp_subject)`` would let a replacement IdP
  that reuses a subject string inherit an old principal. The unique key becomes
  ``(tenant_id, idp_issuer, idp_subject)``. 0051's tables have no callers, so no
  row can collide; any pre-existing row gets ``''``, which no login matches.
- ``console_sessions.principal_id`` binds an OIDC session to its principal.
  Nullable, and ``subject`` is left alone: login-code sessions keep working
  unchanged.
- ``oidc_login_attempts`` holds the short-lived, single-use server side of an
  in-flight authorization-code login (hashed state, nonce, PKCE verifier).
  ``expires_at`` is indexed: the unauthenticated login start prunes expired
  attempts and counts live ones on every request, and must not scan the table.

Expand: the release before this one never reads any of it.

Revision ID: 0052
Revises: 0051
Create Date: 2026-09-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0052"
down_revision: str | None = "0051"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
OLD_KEY = "principals_tenant_idp_subject_key"
NEW_KEY = "principals_tenant_issuer_subject_key"
SESSION_FK = "console_sessions_principal_id_fkey"
SESSION_INDEX = "ix_console_sessions_principal_id"
ATTEMPT_EXPIRY_INDEX = "ix_oidc_login_attempts_expires_at"


def upgrade() -> None:
    op.add_column(
        "principals",
        sa.Column("idp_issuer", sa.String(), server_default="", nullable=False),
        schema=SCHEMA,
    )
    op.drop_constraint(OLD_KEY, "principals", type_="unique", schema=SCHEMA)
    op.create_unique_constraint(
        NEW_KEY,
        "principals",
        ["tenant_id", "idp_issuer", "idp_subject"],
        schema=SCHEMA,
    )

    op.add_column(
        "console_sessions",
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=SCHEMA,
    )
    op.create_foreign_key(
        SESSION_FK,
        "console_sessions",
        "principals",
        ["principal_id"],
        ["id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="CASCADE",
    )
    op.create_index(SESSION_INDEX, "console_sessions", ["principal_id"], schema=SCHEMA)

    op.create_table(
        "oidc_login_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("state_hash", sa.String(), nullable=False),
        sa.Column("nonce", sa.String(), nullable=False),
        sa.Column("code_verifier", sa.String(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("state_hash", name="oidc_login_attempts_state_hash_key"),
        schema=SCHEMA,
    )
    op.create_index(
        ATTEMPT_EXPIRY_INDEX, "oidc_login_attempts", ["expires_at"], schema=SCHEMA
    )


def downgrade() -> None:
    op.drop_index(ATTEMPT_EXPIRY_INDEX, table_name="oidc_login_attempts", schema=SCHEMA)
    op.drop_table("oidc_login_attempts", schema=SCHEMA)

    op.drop_index(SESSION_INDEX, table_name="console_sessions", schema=SCHEMA)
    op.drop_constraint(SESSION_FK, "console_sessions", type_="foreignkey", schema=SCHEMA)
    op.drop_column("console_sessions", "principal_id", schema=SCHEMA)

    # Restoring the narrower key fails loudly if two issuers now share a
    # subject in one tenant, rather than silently deleting either principal.
    op.drop_constraint(NEW_KEY, "principals", type_="unique", schema=SCHEMA)
    op.create_unique_constraint(
        OLD_KEY, "principals", ["tenant_id", "idp_subject"], schema=SCHEMA
    )
    op.drop_column("principals", "idp_issuer", schema=SCHEMA)

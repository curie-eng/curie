"""Owner coding documents and default-enabled tenant policy (#5590, ADR 0278).

Revision ID: 0165
Revises: 0164
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0165"
down_revision: str = "0164"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"


def upgrade() -> None:
    op.create_table(
        "requester_coding_profiles",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("text", sa.Text(), nullable=True),
        sa.Column("revision", sa.BigInteger(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=True),
        sa.Column("format_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("tenant_id", "principal_id", name="requester_coding_profiles_pkey"),
        sa.ForeignKeyConstraint(
            ["tenant_id", "principal_id"],
            [f"{S}.principals.tenant_id", f"{S}.principals.id"],
            name="requester_coding_profiles_principal_fkey",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "revision >= 1 AND revision <= 9223372036854775807",
            name="requester_coding_profiles_revision_ck",
        ),
        sa.CheckConstraint("format_version = 1", name="requester_coding_profiles_format_ck"),
        sa.CheckConstraint(
            "((text IS NULL AND sha256 IS NULL) OR (text IS NOT NULL AND sha256 IS NOT NULL "
            "AND octet_length(text) BETWEEN 1 AND 16384 AND position(chr(13) in text) = 0 "
            "AND sha256 ~ '^[0-9a-f]{64}$' "
            "AND sha256 = encode(sha256(convert_to(text, 'UTF8')), 'hex'))) IS TRUE",
            name="requester_coding_profiles_text_ck",
        ),
        schema=S,
    )
    op.create_table(
        "requester_coding_policies",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.PrimaryKeyConstraint("tenant_id", name="requester_coding_policies_pkey"),
        sa.ForeignKeyConstraint(["tenant_id"], [f"{S}.tenants.id"], ondelete="CASCADE"),
        schema=S,
    )


def downgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '15000ms'"))
    op.execute(
        sa.text(
            f"LOCK TABLE {S}.requester_coding_profiles, {S}.requester_coding_policies "
            "IN ACCESS EXCLUSIVE MODE"
        )
    )
    present = (
        op.get_bind()
        .exec_driver_sql(
            f"SELECT EXISTS (SELECT 1 FROM {S}.requester_coding_profiles) "
            f"OR EXISTS (SELECT 1 FROM {S}.requester_coding_policies)"
        )
        .scalar_one()
    )
    if present:
        raise RuntimeError("requester_coding_instructions_present")
    op.drop_table("requester_coding_profiles", schema=S)
    op.drop_table("requester_coding_policies", schema=S)

"""Persist explicit HTTP principal authority and lifecycle fencing.

@spec HTTP-PRINCIPAL-ADOPTION-V1-2.
Revision ID: 0132
Revises: 0136
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0132"
down_revision: str = "0136"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-2."""
    op.add_column(
        "tenant_identity_providers",
        sa.Column("http_selection", postgresql.JSONB(none_as_null=True), nullable=True),
        schema="curie",
    )
    op.add_column(
        "tenant_identity_providers",
        sa.Column("lifecycle_generation", sa.BigInteger(), nullable=False, server_default="0"),
        schema="curie",
    )
    op.alter_column("tenant_identity_providers", "governed_domains", nullable=True, schema="curie")
    op.alter_column("tenant_identity_providers", "managed_by", nullable=True, schema="curie")
    op.create_check_constraint(
        "tenant_identity_providers_lifecycle_ck",
        "tenant_identity_providers",
        "lifecycle_generation >= 0",
        schema="curie",
    )
    op.create_check_constraint(
        "tenant_identity_providers_http_object_ck",
        "tenant_identity_providers",
        "http_selection IS NULL OR jsonb_typeof(http_selection) = 'object'",
        schema="curie",
    )
    op.create_check_constraint(
        "tenant_identity_providers_owner_ck",
        "tenant_identity_providers",
        "(http_selection IS NULL AND governed_domains IS NOT NULL AND managed_by IS NOT NULL) "
        "OR (http_selection IS NOT NULL AND lifecycle_generation >= 1)",
        schema="curie",
    )


def downgrade() -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-2. HTTP ownership cannot be undone silently."""
    connection = op.get_bind()
    if connection.scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM curie.tenant_identity_providers "
            "WHERE http_selection IS NOT NULL)"
        )
    ):
        raise RuntimeError("HTTP authority must be removed before schema downgrade")
    op.drop_constraint(
        "tenant_identity_providers_http_object_ck", "tenant_identity_providers", schema="curie"
    )
    op.drop_constraint(
        "tenant_identity_providers_owner_ck", "tenant_identity_providers", schema="curie"
    )
    op.drop_constraint(
        "tenant_identity_providers_lifecycle_ck", "tenant_identity_providers", schema="curie"
    )
    op.alter_column("tenant_identity_providers", "governed_domains", nullable=False, schema="curie")
    op.alter_column("tenant_identity_providers", "managed_by", nullable=False, schema="curie")
    op.drop_column("tenant_identity_providers", "lifecycle_generation", schema="curie")
    op.drop_column("tenant_identity_providers", "http_selection", schema="curie")

"""Private approval provenance. @spec HTTP-APPROVAL-CONTINUITY-5.

Revision ID: 0137
Revises: 0133
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0137"
down_revision: str = "0133"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-5."""
    op.add_column(
        "approvals", sa.Column("identity_context", sa.Text(), nullable=True), schema="curie"
    )
    op.create_check_constraint(
        "approvals_identity_context_length_ck",
        "approvals",
        "identity_context IS NULL OR octet_length(identity_context) <= 16384",
        schema="curie",
    )


def downgrade() -> None:
    """Do not discard retained authority. @spec HTTP-APPROVAL-CONTINUITY-5."""
    if op.get_bind().scalar(
        sa.text("SELECT EXISTS (SELECT 1 FROM curie.approvals WHERE identity_context IS NOT NULL)")
    ):
        raise RuntimeError("approval_identity_context_present")
    op.drop_constraint("approvals_identity_context_length_ck", "approvals", schema="curie")
    op.drop_column("approvals", "identity_context", schema="curie")

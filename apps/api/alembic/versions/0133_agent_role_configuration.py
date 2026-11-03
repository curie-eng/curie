"""Persist roles without activating consumers. @spec AGENT-ROLE-PERSISTENCE-V1-2/9.

Revision ID: 0133
Revises: 0132
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0133"
down_revision: str = "0132"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-2/9."""
    op.add_column(
        "agent_access_policies",
        sa.Column("role_configuration", postgresql.JSONB(none_as_null=True), nullable=True),
        schema="curie",
    )
    op.create_check_constraint(
        "agent_access_policies_role_object_ck",
        "agent_access_policies",
        "role_configuration IS NULL OR jsonb_typeof(role_configuration) = 'object'",
        schema="curie",
    )


def downgrade() -> None:
    """@spec AGENT-ROLE-PERSISTENCE-V1-9."""
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM curie.agent_access_policies "
            "WHERE role_configuration IS NOT NULL)"
        )
    ):
        raise RuntimeError("agent_role_configuration_present")
    op.drop_constraint(
        "agent_access_policies_role_object_ck",
        "agent_access_policies",
        schema="curie",
    )
    op.drop_column("agent_access_policies", "role_configuration", schema="curie")

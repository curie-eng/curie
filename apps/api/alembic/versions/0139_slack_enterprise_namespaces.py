"""Enterprise Grid organisation namespaces. @spec HTTP-INVOCATION-EXECUTION-21.

Revision ID: 0139
Revises: 0138
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0139"
down_revision: str = "0138"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

S = "curie"


def upgrade() -> None:
    """Widen the closed kind vocabulary. @spec HTTP-INVOCATION-EXECUTION-21."""
    op.drop_constraint("identity_namespaces_kind_ck", "identity_namespaces", schema=S)
    op.create_check_constraint(
        "identity_namespaces_kind_ck",
        "identity_namespaces",
        "kind IN ('slack_workspace', 'email_domain', 'slack_enterprise')",
        schema=S,
    )
    op.create_check_constraint(
        "identity_namespaces_slack_enterprise_key_ck",
        "identity_namespaces",
        "kind <> 'slack_enterprise' OR (provider = 'slack' AND key ~ '^E[A-Z0-9]{2,}$')",
        schema=S,
    )


def downgrade() -> None:
    """Do not discard organisation namespaces. @spec HTTP-INVOCATION-EXECUTION-21."""
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM curie.identity_namespaces "
            "WHERE kind = 'slack_enterprise')"
        )
    ):
        raise RuntimeError("slack_enterprise_namespaces_present")
    op.drop_constraint(
        "identity_namespaces_slack_enterprise_key_ck", "identity_namespaces", schema=S
    )
    op.drop_constraint("identity_namespaces_kind_ck", "identity_namespaces", schema=S)
    op.create_check_constraint(
        "identity_namespaces_kind_ck",
        "identity_namespaces",
        "kind IN ('slack_workspace', 'email_domain')",
        schema=S,
    )

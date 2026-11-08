"""The operator's connector requester identity flag. @spec CONNECTOR-REQUESTER-1.

Revision ID: 0151
Revises: 0149

ADR 0270 decision 1. Every existing agent is off at version 1: nothing is opted
in by the upgrade. The version is the compare-and-set token every operator
write of the flag names, so it starts at 1 and never goes below.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0151"
down_revision: str = "0149"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def upgrade() -> None:
    """@spec CONNECTOR-REQUESTER-1."""
    op.add_column(
        "agents",
        sa.Column(
            "connector_requester_identity",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "agents",
        sa.Column(
            "connector_requester_identity_version",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "agents_connector_requester_identity_version_ck",
        "agents",
        "connector_requester_identity_version >= 1",
        schema=SCHEMA,
    )


def downgrade() -> None:
    """Drop both columns; an older release has no requester transport to keep.

    @spec CONNECTOR-REQUESTER-1.
    """
    op.drop_constraint(
        "agents_connector_requester_identity_version_ck", "agents", schema=SCHEMA, type_="check"
    )
    op.drop_column("agents", "connector_requester_identity_version", schema=SCHEMA)
    op.drop_column("agents", "connector_requester_identity", schema=SCHEMA)

"""The immutable approval selection snapshot. @spec REQUEST-APPROVAL-4.

Revision ID: 0153
Revises: 0152

ADR 0271 decision 4. Every existing approval reads back ``ordinary`` with no
selection; only a dynamic approval created from a verified approver ticket
carries the requester, connector, tool, key id, nonce, argument digest and
selected approvers, all of them or none (a CHECK). The partial unique index
spends a ticket nonce once per agent. The checks are frozen copies of
``models.Approval``'s, so a later model edit cannot rewrite this revision.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0153"
down_revision: str = "0152"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
_SNAPSHOT_COLUMNS = (
    "selection_requester",
    "selection_connector",
    "selection_tool",
    "selection_key_id",
    "selection_nonce",
    "selection_arguments_sha256",
    "selected_approvers",
)
_MODE_CHECK = "selection_mode IN ('ordinary', 'dynamic')"
_SNAPSHOT_CHECK = (
    "(selection_mode = 'dynamic' AND "
    + " AND ".join(f"{column} IS NOT NULL" for column in _SNAPSHOT_COLUMNS)
    + ") OR (selection_mode = 'ordinary' AND "
    + " AND ".join(f"{column} IS NULL" for column in _SNAPSHOT_COLUMNS)
    + ")"
)


def upgrade() -> None:
    """@spec REQUEST-APPROVAL-4."""
    op.add_column(
        "approvals",
        sa.Column("selection_mode", sa.Text(), nullable=False, server_default="ordinary"),
        schema=SCHEMA,
    )
    op.add_column(
        "approvals",
        sa.Column("selection_requester", postgresql.JSONB(), nullable=True),
        schema=SCHEMA,
    )
    for column in (
        "selection_connector",
        "selection_tool",
        "selection_key_id",
        "selection_nonce",
        "selection_arguments_sha256",
    ):
        op.add_column("approvals", sa.Column(column, sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column(
        "approvals",
        sa.Column("selected_approvers", postgresql.JSONB(), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "approvals_selection_mode_ck",
        "approvals",
        _MODE_CHECK,
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "approvals_selection_snapshot_ck",
        "approvals",
        _SNAPSHOT_CHECK,
        schema=SCHEMA,
    )
    op.create_index(
        "approvals_selection_nonce_key",
        "approvals",
        ["agent_id", "selection_nonce"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("selection_nonce IS NOT NULL"),
    )


def downgrade() -> None:
    """Drop the snapshot; an older release has no dynamic approvals to keep.

    Every approval row stays. @spec REQUEST-APPROVAL-4.
    """
    op.drop_index("approvals_selection_nonce_key", table_name="approvals", schema=SCHEMA)
    op.drop_constraint("approvals_selection_snapshot_ck", "approvals", schema=SCHEMA, type_="check")
    op.drop_constraint("approvals_selection_mode_ck", "approvals", schema=SCHEMA, type_="check")
    for column in reversed(("selection_mode", *_SNAPSHOT_COLUMNS)):
        op.drop_column("approvals", column, schema=SCHEMA)

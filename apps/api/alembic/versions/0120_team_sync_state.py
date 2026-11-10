"""Give every team its directory sync state (#4584, ADR 0220 sections 5 and 6).

A directory adapter replaces a team's whole membership as one complete
snapshot. ``epoch``, ``member_count``, ``digest`` and ``last_synced_at``
record the snapshot the team holds; ``last_attempt_at`` and ``last_error``
record how the latest attempt went, so a failing adapter is visible while the
last snapshot stays authoritative. ``managed_by`` names the adapter that owns
an IdP group's membership.

Expand-only: seven columns with defaults that describe a team no adapter has
synced (epoch 0, no members, no digest, no timestamps), so every existing team
comes through in that state. The numbering skips to 0120 because ``main`` and
``next`` share head 0108, and the number right after it belongs to the next
stable fix.

Revision ID: 0120
Revises: 0108
Create Date: 2026-10-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0120"
down_revision: str | None = "0108"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"

CONSTRAINTS = (
    ("teams_epoch_ck", "epoch >= 0"),
    ("teams_member_count_ck", "member_count >= 0"),
    (
        "teams_sync_state_ck",
        "(epoch = 0 AND member_count = 0 AND digest IS NULL AND last_synced_at IS NULL)"
        " OR (epoch > 0 AND digest IS NOT NULL AND last_synced_at IS NOT NULL)",
    ),
    (
        "teams_managed_by_ck",
        "managed_by IS NULL OR (source = 'idp_group' AND length(managed_by) > 0)",
    ),
    ("teams_last_error_ck", "last_error IS NULL OR length(last_error) BETWEEN 1 AND 200"),
)


def upgrade() -> None:
    op.add_column(
        "teams",
        sa.Column("epoch", sa.Integer(), server_default="0", nullable=False),
        schema=SCHEMA,
    )
    op.add_column(
        "teams",
        sa.Column("member_count", sa.Integer(), server_default="0", nullable=False),
        schema=SCHEMA,
    )
    op.add_column("teams", sa.Column("digest", sa.String(), nullable=True), schema=SCHEMA)
    op.add_column(
        "teams",
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "teams",
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column("teams", sa.Column("last_error", sa.String(), nullable=True), schema=SCHEMA)
    op.add_column("teams", sa.Column("managed_by", sa.String(), nullable=True), schema=SCHEMA)
    for name, condition in CONSTRAINTS:
        op.create_check_constraint(name, "teams", condition, schema=SCHEMA)


def downgrade() -> None:
    for name, _ in reversed(CONSTRAINTS):
        op.drop_constraint(name, "teams", type_="check", schema=SCHEMA)
    for column in (
        "managed_by",
        "last_error",
        "last_attempt_at",
        "last_synced_at",
        "digest",
        "member_count",
        "epoch",
    ):
        op.drop_column("teams", column, schema=SCHEMA)

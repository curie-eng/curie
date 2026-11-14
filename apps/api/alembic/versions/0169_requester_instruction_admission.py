"""Pin requester identity and coding selections at admission (#5591, ADR 0278).

Revision ID: 0169
Revises: 0168
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0169"
down_revision: str = "0168"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"

SELECTION_CHECK = """
((coding_instructions_state = 'absent'
  AND coding_instructions_absence_reason IN
    ('no_profile', 'no_verified_requester', 'tenant_disabled',
     'application_unavailable', 'historical')
  AND coding_instructions_revision IS NULL AND coding_instructions_sha256 IS NULL
  AND coding_instructions_format_version IS NULL)
 OR (coding_instructions_state = 'selected' AND requester_principal_id IS NOT NULL
  AND coding_instructions_absence_reason IS NULL AND coding_instructions_revision >= 1
  AND coding_instructions_sha256 ~ '^[0-9a-f]{64}$'
  AND coding_instructions_format_version = 1)) IS TRUE
"""


def upgrade() -> None:
    op.add_column("work_items", sa.Column("requester_principal_id", postgresql.UUID()), schema=S)
    op.add_column(
        "work_items",
        sa.Column("coding_instructions_state", sa.Text(), nullable=False, server_default="absent"),
        schema=S,
    )
    op.add_column(
        "work_items",
        sa.Column(
            "coding_instructions_absence_reason",
            sa.Text(),
            nullable=True,
            server_default="historical",
        ),
        schema=S,
    )
    for name, kind in (
        ("coding_instructions_revision", sa.BigInteger()),
        ("coding_instructions_sha256", sa.String(64)),
        ("coding_instructions_format_version", sa.Integer()),
    ):
        op.add_column("work_items", sa.Column(name, kind, nullable=True), schema=S)
    op.execute(
        sa.text(
            f"UPDATE {S}.work_items SET requester_principal_id = owner_principal_id "
            "WHERE model_auth_mode = 'personal'"
        )
    )
    op.create_check_constraint(
        "work_items_coding_selection_ck", "work_items", SELECTION_CHECK, schema=S
    )
    op.create_table(
        "work_item_coding_snapshots",
        sa.Column(
            "work_item_id",
            postgresql.UUID(),
            sa.ForeignKey(f"{S}.work_items.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("text", sa.Text(), nullable=False),
        sa.CheckConstraint(
            "octet_length(text) BETWEEN 1 AND 16384 AND position(chr(13) in text) = 0",
            name="work_item_coding_snapshots_text_ck",
        ),
        schema=S,
    )
    op.add_column(
        "execution_requests",
        sa.Column("coding_instructions_selection_id", postgresql.UUID()),
        schema=S,
    )
    op.add_column(
        "execution_requests",
        sa.Column("coding_instructions_evidence", sa.Text(), nullable=False, server_default="none"),
        schema=S,
    )
    op.execute(
        sa.text(
            f"UPDATE {S}.execution_requests SET coding_instructions_selection_id = work_item_id"
        )
    )
    op.create_check_constraint(
        "execution_requests_coding_selection_ck",
        "execution_requests",
        "(coding_instructions_selection_id IS NULL "
        "OR coding_instructions_selection_id = work_item_id) "
        "AND coding_instructions_evidence IN ('none', 'pending', 'applied', 'failed') "
        "AND (coding_instructions_evidence = 'none' "
        "OR coding_instructions_selection_id IS NOT NULL)",
        schema=S,
    )


def downgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '15000ms'"))
    op.execute(
        sa.text(
            f"LOCK TABLE {S}.work_items, {S}.execution_requests, {S}.work_item_coding_snapshots "
            "IN ACCESS EXCLUSIVE MODE"
        )
    )
    present = (
        op.get_bind()
        .exec_driver_sql(
            f"SELECT EXISTS (SELECT 1 FROM {S}.work_items WHERE "
            "coding_instructions_absence_reason IS DISTINCT FROM 'historical' "
            "OR (model_auth_mode = 'service' AND requester_principal_id IS NOT NULL)) "
            f"OR EXISTS (SELECT 1 FROM {S}.work_item_coding_snapshots)"
        )
        .scalar_one()
    )
    if present:
        raise RuntimeError("requester_instruction_selections_present")
    op.drop_constraint("execution_requests_coding_selection_ck", "execution_requests", schema=S)
    op.drop_column("execution_requests", "coding_instructions_evidence", schema=S)
    op.drop_column("execution_requests", "coding_instructions_selection_id", schema=S)
    op.drop_table("work_item_coding_snapshots", schema=S)
    op.drop_constraint("work_items_coding_selection_ck", "work_items", schema=S)
    for name in (
        "coding_instructions_format_version",
        "coding_instructions_sha256",
        "coding_instructions_revision",
        "coding_instructions_absence_reason",
        "coding_instructions_state",
        "requester_principal_id",
    ):
        op.drop_column("work_items", name, schema=S)

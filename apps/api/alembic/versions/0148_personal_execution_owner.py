"""Bind factory executions to a tenant principal and enrollment generation (#5376).

ADR 0268 section 2. Each WorkItem carries one owner tuple, ``tenant_id``,
``model_auth_mode`` (``service`` or ``personal``), ``owner_principal_id`` and
``enrollment_generation``, and every ExecutionRequest snapshots it at insertion.
``personal_claude_enrollments`` is the token-free enrollment header (one row per
tenant principal, a positive generation and CAS version); it holds no Secret
reference.

Existing rows become service rows: ``tenant_id`` is backfilled from the agent
(and onto requests from their WorkItem), and no historical owner is inferred.
``tenant_id`` stays nullable in this expand, so an older application inside the
schema window still inserts a service row through the server default.

Every tuple CHECK is wrapped in ``(...) IS TRUE`` so an UNKNOWN never passes.
Composite foreign keys use MATCH SIMPLE, which skips a key with any NULL member,
so a request carries three of them: ``(work_item_id, model_auth_mode)`` is never
skipped (both are NOT NULL) and keeps a service request out of a personal
WorkItem and the WorkItem's mode fixed while requests exist;
``(work_item_id, tenant_id)`` keeps a populated request tenant equal to its
WorkItem's; ``(work_item_id, tenant_id, owner_principal_id)`` keeps a personal
snapshot equal to its WorkItem's owner and that owner fixed while a request
references it. They keep the default NO ACTION on update, so only
``enrollment_generation`` may move under existing requests.

The downgrade refuses while any personal WorkItem, personal request or
enrollment remains.

Revision ID: 0148
Revises: 0146
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0148"
down_revision: str = "0146"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"

# A bounded wait for the tables the downgrade guard reads, so a busy table
# refuses the downgrade rather than blocking writers behind it indefinitely.
LOCK_TIMEOUT_MS = 15000

MODE_CHECK = "model_auth_mode IN ('service', 'personal')"
# Frozen copy of the owner tuple shape at 0148.
TUPLE_CHECK = (
    "((model_auth_mode = 'service' AND owner_principal_id IS NULL "
    "AND enrollment_generation IS NULL) "
    "OR (model_auth_mode = 'personal' AND tenant_id IS NOT NULL "
    "AND owner_principal_id IS NOT NULL AND enrollment_generation IS NOT NULL "
    "AND enrollment_generation >= 1)) IS TRUE"
)
OWNER_TABLES = ("work_items", "execution_requests")
# The three keys a request's composite foreign keys reference, by name.
WORK_ITEM_KEYS = (
    ("work_items_id_model_auth_mode_key", ["id", "model_auth_mode"]),
    ("work_items_id_tenant_id_key", ["id", "tenant_id"]),
    ("work_items_id_tenant_owner_key", ["id", "tenant_id", "owner_principal_id"]),
)
REQUEST_FOREIGN_KEYS = (
    ("execution_requests_work_item_mode_fkey", ["work_item_id", "model_auth_mode"]),
    ("execution_requests_work_item_tenant_fkey", ["work_item_id", "tenant_id"]),
    (
        "execution_requests_work_item_owner_fkey",
        ["work_item_id", "tenant_id", "owner_principal_id"],
    ),
)


def _add_owner_columns(table: str) -> None:
    op.add_column(
        table, sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=True), schema=S
    )
    op.add_column(
        table,
        sa.Column("model_auth_mode", sa.Text(), nullable=False, server_default="service"),
        schema=S,
    )
    op.add_column(
        table,
        sa.Column("owner_principal_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=S,
    )
    op.add_column(
        table, sa.Column("enrollment_generation", sa.BigInteger(), nullable=True), schema=S
    )


def upgrade() -> None:
    op.create_table(
        "personal_claude_enrollments",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("principal_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("version", sa.BigInteger(), nullable=False, server_default="1"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("id", name="personal_claude_enrollments_pkey"),
        sa.CheckConstraint("generation >= 1", name="personal_claude_enrollments_generation_ck"),
        sa.CheckConstraint("version >= 1", name="personal_claude_enrollments_version_ck"),
        sa.UniqueConstraint(
            "tenant_id", "principal_id", name="personal_claude_enrollments_tenant_principal_key"
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "principal_id"],
            [f"{S}.principals.tenant_id", f"{S}.principals.id"],
            name="personal_claude_enrollments_principal_fkey",
        ),
        schema=S,
    )

    for table in OWNER_TABLES:
        _add_owner_columns(table)
    op.execute(
        sa.text(
            f"UPDATE {S}.work_items AS w SET tenant_id = a.tenant_id FROM {S}.agents AS a "
            "WHERE a.id = w.agent_id AND w.tenant_id IS NULL"
        )
    )
    op.execute(
        sa.text(
            f"UPDATE {S}.execution_requests AS r SET tenant_id = w.tenant_id "
            f"FROM {S}.work_items AS w WHERE w.id = r.work_item_id AND r.tenant_id IS NULL"
        )
    )
    for table in OWNER_TABLES:
        op.create_check_constraint(f"{table}_model_auth_mode_ck", table, MODE_CHECK, schema=S)
        op.create_check_constraint(f"{table}_owner_tuple_ck", table, TUPLE_CHECK, schema=S)

    op.create_foreign_key(
        "work_items_tenant_agent_fkey",
        "work_items",
        "agents",
        ["tenant_id", "agent_id"],
        ["tenant_id", "id"],
        source_schema=S,
        referent_schema=S,
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "work_items_tenant_owner_fkey",
        "work_items",
        "principals",
        ["tenant_id", "owner_principal_id"],
        ["tenant_id", "id"],
        source_schema=S,
        referent_schema=S,
    )
    for name, columns in WORK_ITEM_KEYS:
        op.create_unique_constraint(name, "work_items", columns, schema=S)
    for name, columns in REQUEST_FOREIGN_KEYS:
        op.create_foreign_key(
            name,
            "execution_requests",
            "work_items",
            columns,
            ["id", *columns[1:]],
            source_schema=S,
            referent_schema=S,
            ondelete="CASCADE",
        )


def downgrade() -> None:
    # Held from the check to the drops, so no personal row lands in between.
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    op.execute(
        sa.text(
            f"LOCK TABLE {S}.work_items, {S}.execution_requests, "
            f"{S}.personal_claude_enrollments IN ACCESS EXCLUSIVE MODE"
        )
    )
    present = (
        op.get_bind()
        .exec_driver_sql(
            f"SELECT EXISTS (SELECT 1 FROM {S}.work_items WHERE model_auth_mode <> 'service') "
            f"OR EXISTS (SELECT 1 FROM {S}.execution_requests "
            "WHERE model_auth_mode <> 'service') "
            f"OR EXISTS (SELECT 1 FROM {S}.personal_claude_enrollments)"
        )
        .scalar_one()
    )
    if present:
        raise RuntimeError("personal_execution_records_present")

    for name, _columns in reversed(REQUEST_FOREIGN_KEYS):
        op.drop_constraint(name, "execution_requests", type_="foreignkey", schema=S)
    for name, _columns in reversed(WORK_ITEM_KEYS):
        op.drop_constraint(name, "work_items", type_="unique", schema=S)
    op.drop_constraint("work_items_tenant_owner_fkey", "work_items", type_="foreignkey", schema=S)
    op.drop_constraint("work_items_tenant_agent_fkey", "work_items", type_="foreignkey", schema=S)
    for table in reversed(OWNER_TABLES):
        op.drop_constraint(f"{table}_owner_tuple_ck", table, type_="check", schema=S)
        op.drop_constraint(f"{table}_model_auth_mode_ck", table, type_="check", schema=S)
        for column in (
            "enrollment_generation",
            "owner_principal_id",
            "model_auth_mode",
            "tenant_id",
        ):
            op.drop_column(table, column, schema=S)
    op.drop_table("personal_claude_enrollments", schema=S)

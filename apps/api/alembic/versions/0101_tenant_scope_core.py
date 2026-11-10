"""Tenant scope the core tables; extend Agent into bot identity (#2911, ADR 0166).

``agents``, ``agent_channels``, ``agent_versions`` and ``deployments`` gain a
``tenant_id``. Each column is added nullable, backfilled to the default tenant
0051 provisioned, and only then made NOT NULL. The server default stays: an
N-1 API or worker still serving inside the schema window inserts these rows
without naming a tenant, and those rows belong to the default tenant.

``agents`` also gains the rest of bot identity (ADR 0166 decision 3): a
``status`` (active, paused, draining, retired; default active), an optional
owning team referenced through a tenant-carrying composite key (0052's
discipline), three opaque policy references, and a ``(tenant_id, id)`` key so
a later identity link can name a bot together with its tenant (ADR 0198
decision 5). ``agent_channels`` gains a nullable ``topic_id`` whose foreign key
arrives with the work-item subsystem.

A binding gets no channel identity column: its ``adapter`` already names the
identity (ADR 0168 decision 3, kept by ADR 0198), and that name is now
tenant-scoped through the binding's own ``tenant_id``.

Revision ID: 0101
Revises: 0100
Create Date: 2026-10-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0101"
down_revision: str | None = "0100"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"

# Frozen copy of 0051's DEFAULT_TENANT_ID.
DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"

# crud.agents.delete_agent's order: the agent's bindings, its deployments, its
# versions, then the agent; then teams, which this revision references. Taking
# the strongest lock the revision needs up front, in that order, means no ALTER
# below upgrades a lock while a resolver read holds another of these tables
# (migration_fence's reasoning). The bound makes a busy table refuse the
# revision before anything is mutated; the migrate Job retries it. No single
# order suits every writer: create_agent inserts the agent before its binding,
# so it can meet this lock in a cycle. PostgreSQL's deadlock detector aborts one
# side within deadlock_timeout; an aborted revision has mutated nothing and is
# retried, and an aborted create is an ordinary failed request (migration_fence
# accepts the same outcome).
LOCK_TIMEOUT_MS = 15000
TENANT_TABLES = ("agent_channels", "agents", "agent_versions", "deployments")
LOCKED_TABLES = ("agent_channels", "deployments", "agent_versions", "agents", "teams")

# Frozen copy of curie_api.models.AgentStatus at 0101.
AGENT_STATUSES = ("active", "paused", "draining", "retired")


def _add_tenant_id(table: str) -> None:
    op.add_column(
        table,
        sa.Column(
            "tenant_id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text(f"'{DEFAULT_TENANT_ID}'::uuid"),
            nullable=True,
        ),
        schema=SCHEMA,
    )
    # The constant default already fills existing rows; the backfill is stated
    # rather than left implied by that Postgres behavior.
    op.execute(
        sa.text(
            f"UPDATE {SCHEMA}.{table} SET tenant_id = CAST(:tenant_id AS uuid) "
            "WHERE tenant_id IS NULL"
        ).bindparams(tenant_id=DEFAULT_TENANT_ID)
    )
    op.alter_column(table, "tenant_id", nullable=False, schema=SCHEMA)
    op.create_foreign_key(
        f"{table}_tenant_id_fkey",
        table,
        "tenants",
        ["tenant_id"],
        ["id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
    )


def _lock_tables() -> None:
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    tables = ", ".join(f"{SCHEMA}.{table}" for table in LOCKED_TABLES)
    op.execute(sa.text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE"))


def upgrade() -> None:
    _lock_tables()

    for table in TENANT_TABLES:
        _add_tenant_id(table)

    op.add_column(
        "agents",
        sa.Column("status", sa.String(), server_default="active", nullable=False),
        schema=SCHEMA,
    )
    statuses = ", ".join(f"'{status}'" for status in AGENT_STATUSES)
    op.create_check_constraint(
        "agents_status_ck", "agents", f"status IN ({statuses})", schema=SCHEMA
    )
    op.add_column(
        "agents",
        sa.Column("owning_team_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=SCHEMA,
    )
    op.create_foreign_key(
        "agents_owning_team_fkey",
        "agents",
        "teams",
        ["tenant_id", "owning_team_id"],
        ["tenant_id", "id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
    )
    op.create_index("ix_agents_owning_team_id", "agents", ["owning_team_id"], schema=SCHEMA)
    for column in ("topic_policy_ref", "data_classification_ref", "retention_policy_ref"):
        op.add_column("agents", sa.Column(column, sa.String(), nullable=True), schema=SCHEMA)

    op.create_unique_constraint(
        "agents_tenant_id_id_key", "agents", ["tenant_id", "id"], schema=SCHEMA
    )
    op.add_column(
        "agent_channels",
        sa.Column("topic_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    _lock_tables()
    op.drop_column("agent_channels", "topic_id", schema=SCHEMA)
    op.drop_constraint("agents_tenant_id_id_key", "agents", type_="unique", schema=SCHEMA)
    for column in ("retention_policy_ref", "data_classification_ref", "topic_policy_ref"):
        op.drop_column("agents", column, schema=SCHEMA)
    op.drop_index("ix_agents_owning_team_id", table_name="agents", schema=SCHEMA)
    op.drop_constraint("agents_owning_team_fkey", "agents", type_="foreignkey", schema=SCHEMA)
    op.drop_column("agents", "owning_team_id", schema=SCHEMA)
    op.drop_constraint("agents_status_ck", "agents", type_="check", schema=SCHEMA)
    op.drop_column("agents", "status", schema=SCHEMA)
    for table in reversed(TENANT_TABLES):
        op.drop_constraint(f"{table}_tenant_id_fkey", table, type_="foreignkey", schema=SCHEMA)
        op.drop_column(table, "tenant_id", schema=SCHEMA)

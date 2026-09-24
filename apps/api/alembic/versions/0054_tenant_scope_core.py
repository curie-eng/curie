"""Tenant scope the core tables; extend Agent into bot identity (#2911, ADR 0155 step 6).

``agents``, ``agent_channels``, ``agent_versions`` and ``deployments`` gain a
``tenant_id``. Each column is added nullable, backfilled to the default tenant
0051 provisioned, and only then made NOT NULL. The server default stays: an
N-1 API or worker still serving inside the schema window inserts these rows
without naming a tenant, and those rows belong to the default tenant.

``agents`` also gains the rest of bot identity: a ``status`` (active, paused,
draining, retired; default active), an optional owning team, and three opaque
policy references. ``agent_channels`` gains a nullable
``provider_installation_id``, where NULL keeps today's statically configured
Slack rows exactly as they are, and a nullable ``topic_id`` whose foreign key
arrives with the work-item subsystem.

The owning team and the provider installation are referenced through
tenant-carrying composite keys (0052's discipline), so neither can point into
another tenant. ``provider_installations`` gains the ``(tenant_id, id)`` key
that needs.

Revision ID: 0054
Revises: 0053
Create Date: 2026-09-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0054"
down_revision: str | None = "0053"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"

# Frozen copy of 0051's DEFAULT_TENANT_ID.
DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"

# crud.delete_agent's order: the agent's bindings, then the agent, whose cascade
# reaches its versions and deployments; then the two tables this revision
# constrains or references. Taking the strongest lock the revision needs up
# front, in that order, means no ALTER below upgrades a lock while a resolver
# read holds another of these tables (migration_fence's reasoning). The bound
# makes a busy table refuse the revision before anything is mutated; the
# migrate Job retries it. No single order suits every writer: create_agent
# inserts the agent before its binding, so it can meet this lock in a cycle.
# PostgreSQL's deadlock detector aborts one side within deadlock_timeout; an
# aborted revision has mutated nothing and is retried, and an aborted create
# is an ordinary failed request (migration_fence accepts the same outcome).
LOCK_TIMEOUT_MS = 15000
TENANT_TABLES = ("agent_channels", "agents", "agent_versions", "deployments")
LOCKED_TABLES = (*TENANT_TABLES, "teams", "provider_installations")

# Frozen copy of curie_api.models.AgentStatus at 0054.
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
        "provider_installations_tenant_id_id_key",
        "provider_installations",
        ["tenant_id", "id"],
        schema=SCHEMA,
    )
    op.add_column(
        "agent_channels",
        sa.Column("provider_installation_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=SCHEMA,
    )
    op.create_foreign_key(
        "agent_channels_provider_installation_fkey",
        "agent_channels",
        "provider_installations",
        ["tenant_id", "provider_installation_id"],
        ["tenant_id", "id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
    )
    op.create_index(
        "ix_agent_channels_provider_installation_id",
        "agent_channels",
        ["provider_installation_id"],
        schema=SCHEMA,
    )
    op.add_column(
        "agent_channels",
        sa.Column("topic_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    _lock_tables()
    op.drop_column("agent_channels", "topic_id", schema=SCHEMA)
    op.drop_index(
        "ix_agent_channels_provider_installation_id",
        table_name="agent_channels",
        schema=SCHEMA,
    )
    op.drop_constraint(
        "agent_channels_provider_installation_fkey",
        "agent_channels",
        type_="foreignkey",
        schema=SCHEMA,
    )
    op.drop_column("agent_channels", "provider_installation_id", schema=SCHEMA)
    op.drop_constraint(
        "provider_installations_tenant_id_id_key",
        "provider_installations",
        type_="unique",
        schema=SCHEMA,
    )
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

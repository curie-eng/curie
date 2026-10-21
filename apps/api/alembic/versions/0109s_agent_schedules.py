"""Agent-created schedules (ADR 0238, epic self-improving).

Adds four agent columns with defaults, so every existing agent comes out of
the upgrade with schedules off, zone UTC, at most 20 active schedules and a
15-minute minimum interval. Adds three tables:

``agent_schedules`` is the mutable head: one recurring (``cron``) or one-shot
(``run_at``) schedule, exactly one of the two, with its destination binding
stored by kind, address and adapter. ``revision`` is the configuration
revision every change compare-and-sets. ``idempotency_key`` is unique per
agent so a retried create returns the row it made.

``agent_schedule_revisions`` is the insert-only change log, keyed by
(schedule_id, seq); several entries share one ``revision`` because pause,
resume, cancel and completion are logged without moving it.

``agent_schedule_runs`` is the immutable snapshot of what one hook run fired,
keyed by its ``hook_runs`` row.

Expand-only: four defaulted columns and three new, empty tables.

Revision ID: selfimp_sched_0001
Revises: selfimp_skills_0001
Create Date: 2026-10-20
"""

import uuid
from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "selfimp_sched_0001"
down_revision: str | None = "selfimp_skills_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def _timestamp(name: str) -> sa.Column[datetime]:
    return sa.Column(
        name, sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


def _agent_fk() -> sa.Column[uuid.UUID]:
    return sa.Column(
        "agent_id",
        postgresql.UUID(as_uuid=True),
        sa.ForeignKey(f"{SCHEMA}.agents.id", ondelete="CASCADE"),
        nullable=False,
    )


def upgrade() -> None:
    op.add_column(
        "agents",
        sa.Column("agent_schedules", sa.Boolean(), server_default=sa.false(), nullable=False),
        schema=SCHEMA,
    )
    op.add_column(
        "agents",
        sa.Column("schedule_timezone", sa.Text(), server_default=sa.text("'UTC'"), nullable=False),
        schema=SCHEMA,
    )
    op.add_column(
        "agents",
        sa.Column(
            "schedule_max_active", sa.Integer(), server_default=sa.text("20"), nullable=False
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "agents",
        sa.Column(
            "schedule_min_interval_minutes",
            sa.Integer(),
            server_default=sa.text("15"),
            nullable=False,
        ),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "agents_schedule_max_active_ck",
        "agents",
        "schedule_max_active BETWEEN 1 AND 100",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "agents_schedule_min_interval_ck",
        "agents",
        "schedule_min_interval_minutes BETWEEN 1 AND 1440",
        schema=SCHEMA,
    )

    op.create_table(
        "agent_schedules",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        _agent_fk(),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("cron", sa.Text(), nullable=True),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("timezone", sa.Text(), nullable=False),
        sa.Column("destination_kind", sa.Text(), nullable=False),
        sa.Column("destination_address", sa.Text(), nullable=False),
        sa.Column("destination_adapter", sa.Text(), nullable=True),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("created_source", sa.Text(), nullable=False),
        sa.Column("created_conversation", sa.Text(), nullable=True),
        sa.Column("created_turn", sa.Text(), nullable=True),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.Column("updated_by", sa.Text(), nullable=True),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancelled_by", sa.Text(), nullable=True),
        sa.Column("fired_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(cron IS NULL) <> (run_at IS NULL)", name="agent_schedules_one_kind_ck"
        ),
        sa.CheckConstraint(
            "state IN ('active', 'cancelled', 'done')", name="agent_schedules_state_ck"
        ),
        sa.CheckConstraint(
            "created_source IN ('agent', 'operator')", name="agent_schedules_source_ck"
        ),
        sa.CheckConstraint("revision >= 1", name="agent_schedules_revision_ck"),
        sa.UniqueConstraint(
            "agent_id", "idempotency_key", name="uq_agent_schedules_idempotency_key"
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_agent_schedules_agent_state",
        "agent_schedules",
        ["agent_id", "state"],
        schema=SCHEMA,
    )

    op.create_table(
        "agent_schedule_revisions",
        sa.Column(
            "schedule_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.agent_schedules.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        # The schedule's read-back state after this entry (paused included).
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("cron", sa.Text(), nullable=True),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("timezone", sa.Text(), nullable=False),
        sa.Column("author", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("conversation", sa.Text(), nullable=True),
        sa.Column("turn", sa.Text(), nullable=True),
        _timestamp("created_at"),
        sa.PrimaryKeyConstraint("schedule_id", "seq"),
        sa.CheckConstraint(
            "action IN ('create', 'update', 'pause', 'resume', 'cancel', 'revert', 'complete')",
            name="agent_schedule_revisions_action_ck",
        ),
        sa.CheckConstraint(
            "state IN ('active', 'paused', 'cancelled', 'done')",
            name="agent_schedule_revisions_state_ck",
        ),
        sa.CheckConstraint(
            "source IN ('agent', 'operator', 'scheduler')",
            name="agent_schedule_revisions_source_ck",
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "agent_schedule_runs",
        sa.Column(
            "hook_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.hook_runs.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "schedule_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.agent_schedules.id", ondelete="CASCADE"),
            nullable=False,
        ),
        _agent_fk(),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("prompt", sa.Text(), nullable=False),
        sa.Column("cron", sa.Text(), nullable=True),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("timezone", sa.Text(), nullable=False),
        sa.Column("destination_kind", sa.Text(), nullable=False),
        sa.Column("destination_address", sa.Text(), nullable=False),
        sa.Column("destination_adapter", sa.Text(), nullable=True),
        _timestamp("created_at"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_agent_schedule_runs_schedule",
        "agent_schedule_runs",
        ["schedule_id", "revision"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_agent_schedule_runs_schedule", table_name="agent_schedule_runs", schema=SCHEMA
    )
    op.drop_table("agent_schedule_runs", schema=SCHEMA)
    op.drop_table("agent_schedule_revisions", schema=SCHEMA)
    op.drop_index("ix_agent_schedules_agent_state", table_name="agent_schedules", schema=SCHEMA)
    op.drop_table("agent_schedules", schema=SCHEMA)
    op.drop_constraint("agents_schedule_min_interval_ck", "agents", schema=SCHEMA)
    op.drop_constraint("agents_schedule_max_active_ck", "agents", schema=SCHEMA)
    op.drop_column("agents", "schedule_min_interval_minutes", schema=SCHEMA)
    op.drop_column("agents", "schedule_max_active", schema=SCHEMA)
    op.drop_column("agents", "schedule_timezone", schema=SCHEMA)
    op.drop_column("agents", "agent_schedules", schema=SCHEMA)

"""Record triage delegations and their single-use implementation handoffs (#5588).

Revision ID: 0171
Revises: 0168
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0171"
down_revision: str = "0168"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_UUID = postgresql.UUID(as_uuid=True)


def upgrade() -> None:
    op.create_table(
        "factory_triage_delegations",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column(
            "work_item_id",
            _UUID,
            sa.ForeignKey("curie.work_items.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_execution_request_id", _UUID, nullable=False, unique=True),
        *[
            sa.Column(name, sa.Text(), nullable=False)
            for name in (
                "actor_id",
                "actor_login",
                "selection_event",
                "tracker_kind",
                "tracker_host",
                "tracker_scope_id",
                "tracker_issue_id",
                "repository_id",
                "result_policy_digest",
                "target_label",
            )
        ],
        sa.Column("installation_id", sa.BigInteger(), nullable=True),
        sa.Column("source_fingerprint", sa.Text(), nullable=True),
        sa.Column("target_binding_id", _UUID, nullable=True),
        sa.Column("target_binding_generation", sa.Integer(), nullable=True),
        sa.Column("target_agent_id", _UUID, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(target_binding_id IS NULL) = (target_agent_id IS NULL) "
            "AND (target_binding_id IS NULL) = (target_binding_generation IS NULL)",
            name="factory_triage_delegations_target_ck",
        ),
        schema="curie",
    )
    op.create_table(
        "factory_triage_handoffs",
        sa.Column("id", _UUID, primary_key=True),
        sa.Column(
            "delegation_id",
            _UUID,
            sa.ForeignKey("curie.factory_triage_delegations.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("source_execution_request_id", _UUID, nullable=False, unique=True),
        *[
            sa.Column(name, sa.Text(), nullable=False)
            for name in (
                "output_tracker_kind",
                "output_tracker_host",
                "output_scope_id",
                "output_issue_id",
                "output_url",
                "output_body_sha256",
            )
        ],
        sa.Column("implementation_request_id", _UUID, nullable=True),
        sa.Column("outcome", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "label_cleanup_owed", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column("lease_owner", sa.Text(), nullable=True),
        *[
            sa.Column(name, sa.DateTime(timezone=True), nullable=True)
            for name in (
                "associated_at",
                "labelled_at",
                "lease_expires_at",
                "settled_at",
            )
        ],
        *[
            sa.Column(name, sa.DateTime(timezone=True), nullable=False)
            for name in ("next_attempt_at", "deadline_at")
        ],
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "output_tracker_host",
            "output_scope_id",
            "output_issue_id",
            name="factory_triage_handoffs_output_key",
        ),
        sa.CheckConstraint(
            "outcome IS NULL OR outcome IN ('admitted', 'refused')",
            name="factory_triage_handoffs_outcome_ck",
        ),
        sa.CheckConstraint(
            "(outcome = 'admitted') = (implementation_request_id IS NOT NULL) "
            "AND (outcome = 'refused') = (reason IS NOT NULL) "
            "AND (outcome IS NULL) = (settled_at IS NULL)",
            name="factory_triage_handoffs_settled_ck",
        ),
        schema="curie",
    )
    op.create_index(
        "ix_factory_triage_handoffs_owed",
        "factory_triage_handoffs",
        ["next_attempt_at"],
        schema="curie",
        postgresql_where=sa.text("outcome IS NULL OR label_cleanup_owed"),
    )


def downgrade() -> None:
    bind = op.get_bind()
    count = bind.scalar(sa.text("SELECT count(*) FROM curie.factory_triage_delegations"))
    if count is None or count > 0:
        raise RuntimeError("cannot downgrade while factory triage delegations exist")
    op.drop_table("factory_triage_handoffs", schema="curie")
    op.drop_table("factory_triage_delegations", schema="curie")

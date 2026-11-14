"""Answer refused trigger-label additions through a leased outbox.

Revision ID: 0168
Revises: 0167
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0168"
down_revision: str = "0167"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "factory_intake_notices",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        *[
            sa.Column(name, sa.Text(), nullable=False)
            for name in (
                "tracker_kind",
                "tracker_host",
                "tracker_scope_id",
                "tracker_issue_id",
                "repository_path",
                "issue_url",
                "label",
                "reason",
            )
        ],
        sa.Column("label_event_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "agent_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.agents.id", ondelete="SET NULL"),
            nullable=True,
        ),
        *[
            sa.Column(name, sa.Text(), nullable=True)
            for name in (
                "actor_login",
                "publication_url",
                "lease_owner",
                "last_error",
            )
        ],
        *[
            sa.Column(name, sa.DateTime(timezone=True), nullable=True)
            for name in (
                "label_event_at",
                "posted_at",
                "not_before",
                "lease_expires_at",
            )
        ],
        sa.Column(
            "observed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("comment_id", sa.BigInteger(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint(
            "tracker_host",
            "tracker_scope_id",
            "tracker_issue_id",
            "label_event_id",
            name="factory_intake_notices_event_key",
        ),
        sa.CheckConstraint(
            "reason IN ('actor_unverifiable', 'actor_bot', 'actor_own_app', 'actor_no_permission', "
            "'personal_owner_required', 'binding_ambiguous', 'publication_completed', "
            "'installation_changed')",
            name="factory_intake_notices_reason_ck",
        ),
        schema="curie",
    )
    op.create_index(
        "ix_factory_intake_notices_owed",
        "factory_intake_notices",
        ["observed_at"],
        schema="curie",
        postgresql_where=sa.text("posted_at IS NULL"),
    )


def downgrade() -> None:
    count = op.get_bind().scalar(sa.text("SELECT count(*) FROM curie.factory_intake_notices"))
    if count is None or count > 0:
        raise RuntimeError("cannot downgrade while factory intake notices exist")
    op.drop_table("factory_intake_notices", schema="curie")

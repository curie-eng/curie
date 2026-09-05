"""Add verified publication identity and review revision reservations.

Revision ID: 0042
Revises: 0041
Create Date: 2026-09-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0042"
down_revision: str | None = "0041"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
SCHEMA = "curie"
LINEAGES = "thread_publication_lineages"
RESERVATIONS = "publication_review_reservations"


def upgrade() -> None:
    # NULL preserves uncertainty: a current name/number cannot authenticate the
    # original repository after deletion/recreation or an App reinstall.
    for name, kind in (
        ("binding_id", postgresql.UUID(as_uuid=True)),
        ("binding_generation", sa.Integer()),
        ("reply_conversation_id", sa.String()),
        ("github_repository_id", sa.BigInteger()),
        ("github_installation_id", sa.BigInteger()),
        ("github_pr_node_id", sa.String()),
        ("base_ref", sa.String()),
    ):
        op.add_column(LINEAGES, sa.Column(name, kind, nullable=True), schema=SCHEMA)
    op.create_foreign_key(
        "fk_publication_lineage_binding",
        LINEAGES,
        "agent_channels",
        ["binding_id"],
        ["id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="SET NULL",
    )
    op.create_check_constraint(
        "thread_publication_lineages_github_identity_ck",
        LINEAGES,
        "(github_repository_id IS NULL AND github_installation_id IS NULL "
        "AND github_pr_node_id IS NULL AND base_ref IS NULL) "
        "OR (github_repository_id IS NOT NULL AND github_repository_id > 0 "
        "AND github_installation_id IS NOT NULL AND github_installation_id > 0 "
        "AND github_pr_node_id IS NOT NULL AND length(github_pr_node_id) > 0 "
        "AND pr_number IS NOT NULL AND base_ref IS NOT NULL AND length(base_ref) > 0)",
        schema=SCHEMA,
    )
    op.create_index(
        "uq_publication_github_pr_owner",
        LINEAGES,
        ["github_repository_id", "pr_number"],
        unique=True,
        schema=SCHEMA,
    )
    op.create_index(
        "uq_active_publication_github_conversation",
        LINEAGES,
        ["agent_id", "conversation_id", "github_repository_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("status = 'open'"),
    )
    op.create_table(
        RESERVATIONS,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("origin_key", sa.String(), nullable=False, unique=True),
        sa.Column(
            "lineage_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.{LINEAGES}.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("lineage_version", sa.Integer(), nullable=False),
        sa.Column("expected_head_sha", sa.String(), nullable=False),
        sa.Column("revision_number", sa.Integer(), nullable=False),
        sa.Column("binding_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("binding_generation", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="reserved"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "status IN ('reserved', 'consumed', 'cancelled')",
            name="publication_review_reservations_status_ck",
        ),
        sa.CheckConstraint(
            "version >= 1 AND revision_number >= 1 AND lineage_version >= 1",
            name="publication_review_reservations_versions_ck",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_publication_review_reservations_lineage_id", RESERVATIONS, ["lineage_id"], schema=SCHEMA
    )
    op.create_index(
        "uq_reserved_review_per_lineage",
        RESERVATIONS,
        ["lineage_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("status = 'reserved'"),
    )


def downgrade() -> None:
    active = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM curie.publication_review_reservations "
                "WHERE status='reserved') OR EXISTS (SELECT 1 FROM curie.publications "
                "WHERE status IN ('pending','approved','launching','running'))"
            )
        )
        .scalar_one()
    )
    if active:
        raise RuntimeError("cannot remove review authority while revisions are active")
    op.drop_table(RESERVATIONS, schema=SCHEMA)
    op.drop_index("uq_active_publication_github_conversation", table_name=LINEAGES, schema=SCHEMA)
    op.drop_index("uq_publication_github_pr_owner", table_name=LINEAGES, schema=SCHEMA)
    op.drop_constraint("thread_publication_lineages_github_identity_ck", LINEAGES, schema=SCHEMA)
    op.drop_constraint("fk_publication_lineage_binding", LINEAGES, schema=SCHEMA)
    for name in (
        "base_ref",
        "github_pr_node_id",
        "github_installation_id",
        "github_repository_id",
        "reply_conversation_id",
        "binding_generation",
        "binding_id",
    ):
        op.drop_column(LINEAGES, name, schema=SCHEMA)

"""Store optional UI evidence for a publication until it is published.

Three tables hang off a publication and its lineage. The image bytes live in a
nullable bytea column cleared once the row is terminal, as
``publications.patch_bytes`` is. Nothing here changes a publication table.

Revision ID: 0146
Revises: 0145
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0146"
down_revision: str = "0145"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"

EVIDENCE_STATUSES = (
    "receiving",
    "ready",
    "publishing",
    "published",
    "unsupported",
    "failed",
    "superseded",
    "abandoned",
)


def _in(column: str, values: Sequence[str]) -> str:
    return f"{column} IN ({', '.join(repr(value) for value in values)})"


def upgrade() -> None:
    op.create_table(
        "publication_ui_evidence",
        sa.Column(
            "publication_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.publications.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "lineage_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.thread_publication_lineages.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("index_sha256", sa.Text(), nullable=False),
        sa.Column("manifest_state", sa.Text(), nullable=False),
        sa.Column("manifest_refusal", sa.Text(), nullable=True),
        sa.Column("manifest_status", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("base_sha", sa.Text(), nullable=False),
        *[
            sa.Column(name, sa.Text(), nullable=True)
            for name in (
                "manifest_base_sha",
                "manifest_head_sha",
                "manifest_candidate_tree",
                "candidate_tree",
                "head_tree",
            )
        ],
        sa.Column("candidate_dirty", sa.Boolean(), nullable=True),
        sa.Column("warnings", postgresql.JSONB(), nullable=False, server_default="[]"),
        sa.Column("total_bytes", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("upload_deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("publish_deadline_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_owner", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("outcome", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("evidence_commit_sha", sa.Text(), nullable=True),
        sa.Column("comment_written_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("terminal_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            _in("status", EVIDENCE_STATUSES), name="publication_ui_evidence_status_ck"
        ),
        sa.CheckConstraint(
            _in("manifest_state", ("valid", "invalid")),
            name="publication_ui_evidence_manifest_state_ck",
        ),
        sa.CheckConstraint(
            "manifest_status IS NULL OR "
            + _in("manifest_status", ("complete", "partial", "skipped", "failed")),
            name="publication_ui_evidence_manifest_status_ck",
        ),
        sa.CheckConstraint(
            "total_bytes >= 0 AND total_bytes <= 104857600",
            name="publication_ui_evidence_total_bytes_ck",
        ),
        sa.CheckConstraint(
            "version >= 1 AND attempts >= 0", name="publication_ui_evidence_version_ck"
        ),
        schema=S,
    )
    op.create_index(
        "ix_publication_ui_evidence_status_lease",
        "publication_ui_evidence",
        ["status", "lease_expires_at"],
        schema=S,
    )
    op.create_index(
        "ix_publication_ui_evidence_lineage_id",
        "publication_ui_evidence",
        ["lineage_id"],
        schema=S,
    )
    op.create_table(
        "publication_ui_evidence_images",
        sa.Column(
            "publication_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.publication_ui_evidence.publication_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("ordinal", sa.Integer(), primary_key=True),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("label", sa.Text(), nullable=False),
        # Null only on a refused entry whose manifest state was ill-typed.
        sa.Column("state", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("sha256", sa.Text(), nullable=True),
        sa.Column("refusal", sa.Text(), nullable=True),
        sa.Column("bound", sa.Boolean(), nullable=False),
        sa.Column("content", sa.LargeBinary(), nullable=True),
        sa.Column("stored_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("url", sa.Text(), nullable=True),
        sa.CheckConstraint("ordinal >= 0", name="publication_ui_evidence_images_ordinal_ck"),
        sa.CheckConstraint(
            "char_length(path) <= 512", name="publication_ui_evidence_images_path_ck"
        ),
        sa.CheckConstraint(
            "char_length(label) <= 200", name="publication_ui_evidence_images_label_ck"
        ),
        sa.CheckConstraint(
            _in("state", ("before", "after")), name="publication_ui_evidence_images_state_ck"
        ),
        sa.CheckConstraint(
            "size_bytes IS NULL OR (size_bytes >= 0 AND size_bytes <= 10485760)",
            name="publication_ui_evidence_images_size_ck",
        ),
        schema=S,
    )
    op.create_table(
        "publication_lineage_ui_evidence",
        sa.Column(
            "lineage_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("curie.thread_publication_lineages.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("ref_commit_sha", sa.Text(), nullable=True),
        # Set before the code host is asked to store a ref, so a crash before
        # ref_commit_sha is recorded still leaves the ref findable for cleanup.
        sa.Column("ref_pending", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("comment_revision", sa.Integer(), nullable=True),
        # Set before a comment is posted and cleared when it is recorded, so a
        # crash between the two still lets a later revision replace it.
        sa.Column("comment_pending", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("comment_kind", sa.Text(), nullable=True),
        sa.Column("lease_owner", sa.Text(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(
            "comment_kind IS NULL OR "
            + _in("comment_kind", ("evidence", "unsupported", "stale", "failed")),
            name="publication_lineage_ui_evidence_comment_kind_ck",
        ),
        sa.CheckConstraint("version >= 1", name="publication_lineage_ui_evidence_version_ck"),
        schema=S,
    )


def downgrade() -> None:
    op.drop_table("publication_lineage_ui_evidence", schema=S)
    op.drop_table("publication_ui_evidence_images", schema=S)
    op.drop_index(
        "ix_publication_ui_evidence_lineage_id", table_name="publication_ui_evidence", schema=S
    )
    op.drop_index(
        "ix_publication_ui_evidence_status_lease", table_name="publication_ui_evidence", schema=S
    )
    op.drop_table("publication_ui_evidence", schema=S)

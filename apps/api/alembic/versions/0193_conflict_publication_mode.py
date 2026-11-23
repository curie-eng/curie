"""Bind an approved conflict publication to its ordered Git parents.

Revision ID: 0193
Revises: 0195
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0193"
down_revision: str = "0195"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

LOCK_TIMEOUT_MS = 15000


def upgrade() -> None:
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'")
    op.execute(
        """
        ALTER TABLE curie.publications ADD COLUMN publication_mode text,
          ADD COLUMN expected_merge_sha text,
          ADD CONSTRAINT publications_expected_merge_sha_ck CHECK (
            expected_merge_sha IS NULL OR (
              publication_mode IS NOT DISTINCT FROM 'base_conflict_merge'
              AND expected_merge_sha ~ '^[0-9a-f]{40,64}$'
              AND expected_merge_sha <> expected_prior_head AND expected_merge_sha <> base_sha)),
          ADD CONSTRAINT publications_conflict_mode_ck CHECK (
            publication_mode IS NULL OR (
              publication_mode = 'base_conflict_merge' AND expected_prior_head IS NOT NULL
              AND base_sha <> expected_prior_head
              AND revision_number IS NOT NULL AND revision_number > 1
              AND CASE WHEN patch_bytes IS NULL
                       THEN status IN ('succeeded', 'failed', 'denied', 'expired')
                       ELSE octet_length(patch_bytes) > 0 END))
        """
    )
    op.execute(
        """
        CREATE FUNCTION curie.protect_conflict_publication() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.expected_merge_sha IS NOT NULL
             AND NEW.expected_merge_sha IS DISTINCT FROM OLD.expected_merge_sha THEN
            RAISE EXCEPTION 'prepared conflict merge SHA is immutable' USING ERRCODE = '23514';
          END IF;
          IF NEW.publication_mode IS DISTINCT FROM OLD.publication_mode OR
             (OLD.publication_mode = 'base_conflict_merge' AND
              (NEW.expected_prior_head IS DISTINCT FROM OLD.expected_prior_head OR
               NEW.base_sha IS DISTINCT FROM OLD.base_sha OR
               (NEW.patch_bytes IS NOT NULL AND NEW.patch_bytes IS DISTINCT FROM OLD.patch_bytes)))
          THEN
            RAISE EXCEPTION 'approved conflict publication parents, mode and patch are immutable'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER protect_conflict_publication
        BEFORE UPDATE OF publication_mode, base_sha, expected_prior_head,
                         patch_bytes, expected_merge_sha
        ON curie.publications
        FOR EACH ROW EXECUTE FUNCTION curie.protect_conflict_publication()
        """
    )


def downgrade() -> None:
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'")
    op.execute("LOCK TABLE curie.publications IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT FROM curie.publications WHERE publication_mode IS NOT NULL) THEN
            RAISE EXCEPTION 'cannot discard the publication mode of a conflict repair';
          END IF;
        END $$
        """
    )
    op.execute("DROP TRIGGER protect_conflict_publication ON curie.publications")
    op.execute("DROP FUNCTION curie.protect_conflict_publication()")
    op.drop_column("publications", "expected_merge_sha", schema="curie")
    op.drop_column("publications", "publication_mode", schema="curie")

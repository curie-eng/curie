"""Bounded private workspace recovery metadata.

Revision ID: 0141
Revises: 0139
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0141"
down_revision: str = "0139"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    statements = (
        "ALTER TABLE curie.work_items ADD COLUMN workspace_recovery_outcome varchar(128), "
        "ADD COLUMN workspace_recovery_outcome_at timestamptz",
        "CREATE TABLE curie.workspace_recovery_installation "
        "(id integer PRIMARY KEY DEFAULT 1, "
        "CONSTRAINT recovery_installation_singleton_ck CHECK (id = 1))",
        "INSERT INTO curie.workspace_recovery_installation (id) VALUES (1)",
        """CREATE TABLE curie.workspace_recovery_groups (
            work_item_id uuid PRIMARY KEY REFERENCES curie.work_items(id) ON DELETE RESTRICT,
            repository_provider text NOT NULL,
            repository_host text NOT NULL,
            repository_project_id text NOT NULL,
            repository_path text NOT NULL,
            base_commit varchar(64) NOT NULL,
            target_branch text NOT NULL,
            last_good_checkpoint_id uuid
        )""",
        """CREATE TABLE curie.workspace_recovery_checkpoints (
            id uuid PRIMARY KEY,
            work_item_id uuid NOT NULL REFERENCES
                curie.workspace_recovery_groups(work_item_id) ON DELETE RESTRICT,
            source_request_id uuid NOT NULL REFERENCES
                curie.execution_requests(id) ON DELETE RESTRICT,
            runtime_epoch bigint NOT NULL,
            dispatch_generation bigint NOT NULL,
            idempotency_key varchar(256) NOT NULL,
            content_sha256 varchar(64) NOT NULL,
            version bigint NOT NULL DEFAULT 1,
            includes_base boolean NOT NULL DEFAULT false,
            status varchar(16) NOT NULL DEFAULT 'staging',
            capture_deadline timestamptz NOT NULL,
            staging_expires_at timestamptz NOT NULL,
            completed_at timestamptz,
            expires_at timestamptz,
            CONSTRAINT recovery_checkpoint_group_id_key UNIQUE (work_item_id, id),
            CONSTRAINT recovery_checkpoint_idempotency_key
                UNIQUE (source_request_id, idempotency_key),
            CONSTRAINT recovery_checkpoint_status_ck
                CHECK (status IN ('staging', 'complete', 'tombstoned')),
            CONSTRAINT recovery_checkpoint_authority_ck
                CHECK (runtime_epoch >= 1 AND dispatch_generation >= 1 AND version >= 1),
            CONSTRAINT recovery_checkpoint_idempotency_length_ck
                CHECK (char_length(idempotency_key) BETWEEN 1 AND 256),
            CONSTRAINT recovery_checkpoint_digest_ck
                CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
            CONSTRAINT recovery_checkpoint_completion_ck CHECK (
                (completed_at IS NULL) = (expires_at IS NULL) AND
                (status <> 'complete' OR completed_at IS NOT NULL))
        )""",
        "CREATE UNIQUE INDEX recovery_checkpoint_one_staging ON "
        "curie.workspace_recovery_checkpoints(work_item_id) WHERE status = 'staging'",
        "ALTER TABLE curie.workspace_recovery_groups ADD CONSTRAINT "
        "recovery_group_last_good_fkey FOREIGN KEY (work_item_id,last_good_checkpoint_id) "
        "REFERENCES curie.workspace_recovery_checkpoints(work_item_id,id)",
        """CREATE TABLE curie.workspace_recovery_objects (
            id uuid PRIMARY KEY,
            work_item_id uuid NOT NULL REFERENCES
                curie.workspace_recovery_groups(work_item_id) ON DELETE RESTRICT,
            checkpoint_id uuid,
            kind varchar(16) NOT NULL,
            key varchar(512) NOT NULL UNIQUE,
            size_bytes bigint NOT NULL,
            sha256 varchar(64) NOT NULL,
            status varchar(16) NOT NULL DEFAULT 'staging',
            staging_expires_at timestamptz,
            claim_id uuid,
            collector_id varchar(256),
            claimed_until timestamptz,
            version bigint NOT NULL DEFAULT 0,
            CONSTRAINT recovery_object_checkpoint_fkey FOREIGN KEY(work_item_id, checkpoint_id)
                REFERENCES curie.workspace_recovery_checkpoints(work_item_id,id) ON DELETE RESTRICT,
            CONSTRAINT recovery_object_checkpoint_kind_key UNIQUE (checkpoint_id,kind),
            CONSTRAINT recovery_object_kind_ck CHECK (
                kind IN ('base','patch','manifest') AND
                ((kind = 'base') = (checkpoint_id IS NULL))),
            CONSTRAINT recovery_object_descriptor_ck CHECK (
                size_bytes >= 0 AND sha256 ~ '^[0-9a-f]{64}$'),
            CONSTRAINT recovery_object_status_ck CHECK (
                status IN ('staging','complete','tombstoned','claimed')),
            CONSTRAINT recovery_object_claim_ck CHECK (
                version >= 0 AND ((status = 'claimed' AND claim_id IS NOT NULL
                AND collector_id IS NOT NULL AND claimed_until IS NOT NULL) OR
                (status <> 'claimed' AND claim_id IS NULL AND collector_id IS NULL
                AND claimed_until IS NULL)))
        )""",
        "CREATE UNIQUE INDEX recovery_object_one_base ON "
        "curie.workspace_recovery_objects(work_item_id) WHERE kind = 'base'",
        "CREATE INDEX recovery_object_group ON curie.workspace_recovery_objects(work_item_id)",
        """CREATE TABLE curie.workspace_recovery_readers (
            id uuid PRIMARY KEY,
            work_item_id uuid NOT NULL REFERENCES
                curie.workspace_recovery_groups(work_item_id) ON DELETE RESTRICT,
            checkpoint_id uuid NOT NULL,
            holder_id varchar(256) NOT NULL,
            expires_at timestamptz NOT NULL,
            CONSTRAINT recovery_reader_checkpoint_fkey FOREIGN KEY (work_item_id,checkpoint_id)
                REFERENCES curie.workspace_recovery_checkpoints(work_item_id,id) ON DELETE RESTRICT,
            CONSTRAINT recovery_reader_holder_key UNIQUE (work_item_id,checkpoint_id,holder_id)
        )""",
    )
    for statement in statements:
        op.execute(statement)


def downgrade() -> None:
    if (
        op.get_bind()
        .exec_driver_sql("SELECT EXISTS (SELECT 1 FROM curie.workspace_recovery_groups)")
        .scalar_one()
    ):
        raise RuntimeError("workspace_recovery_retained_objects_present")
    op.drop_constraint("recovery_group_last_good_fkey", "workspace_recovery_groups", schema="curie")
    for table in (
        "workspace_recovery_readers",
        "workspace_recovery_objects",
        "workspace_recovery_checkpoints",
        "workspace_recovery_groups",
        "workspace_recovery_installation",
    ):
        op.drop_table(table, schema="curie")
    op.drop_column("work_items", "workspace_recovery_outcome_at", schema="curie")
    op.drop_column("work_items", "workspace_recovery_outcome", schema="curie")

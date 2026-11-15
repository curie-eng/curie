"""Bounded private publication candidate metadata (#5428).

Revision ID: 0173
Revises: 0172
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0173"
down_revision: str = "0172"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    statements = (
        "CREATE TABLE curie.publication_candidate_installation "
        "(id integer PRIMARY KEY DEFAULT 1, "
        "CONSTRAINT publication_candidate_installation_singleton_ck CHECK (id = 1))",
        "INSERT INTO curie.publication_candidate_installation (id) VALUES (1)",
        """CREATE TABLE curie.publication_candidate_outcomes (
            work_item_id uuid PRIMARY KEY REFERENCES curie.work_items(id) ON DELETE CASCADE,
            code varchar(128) NOT NULL,
            recorded_at timestamptz NOT NULL,
            CONSTRAINT publication_candidate_outcome_code_ck
                CHECK (char_length(code) BETWEEN 1 AND 128)
        )""",
        """CREATE TABLE curie.publication_candidate_groups (
            work_item_id uuid PRIMARY KEY REFERENCES curie.work_items(id) ON DELETE RESTRICT,
            repository_provider text NOT NULL,
            repository_host text NOT NULL,
            repository_project_id text NOT NULL,
            repository_path text NOT NULL,
            base_commit varchar(64) NOT NULL,
            target_branch text NOT NULL,
            base_sha256 varchar(64) NOT NULL,
            CONSTRAINT publication_candidate_group_identity_ck CHECK (
                char_length(repository_provider) BETWEEN 1 AND 256 AND
                char_length(repository_host) BETWEEN 1 AND 256 AND
                char_length(repository_project_id) BETWEEN 1 AND 256 AND
                char_length(repository_path) BETWEEN 1 AND 2048 AND
                char_length(target_branch) BETWEEN 1 AND 1024),
            CONSTRAINT publication_candidate_group_base_ck CHECK (
                base_commit ~ '^([0-9a-f]{40}|[0-9a-f]{64})$' AND
                base_sha256 ~ '^[0-9a-f]{64}$')
        )""",
        """CREATE TABLE curie.publication_candidates (
            id uuid PRIMARY KEY,
            work_item_id uuid NOT NULL REFERENCES
                curie.publication_candidate_groups(work_item_id) ON DELETE RESTRICT,
            origin_request_id uuid NOT NULL REFERENCES
                curie.execution_requests(id) ON DELETE RESTRICT,
            owner_request_id uuid NOT NULL REFERENCES
                curie.execution_requests(id) ON DELETE RESTRICT,
            previous_owner_request_id uuid REFERENCES
                curie.execution_requests(id) ON DELETE RESTRICT,
            owner_transferred_at timestamptz,
            runtime_epoch bigint NOT NULL,
            dispatch_generation bigint NOT NULL,
            idempotency_key varchar(256) NOT NULL,
            content_sha256 varchar(64) NOT NULL,
            changed_paths jsonb NOT NULL,
            contains_workflow_files boolean NOT NULL,
            title varchar(256) NOT NULL,
            body text NOT NULL,
            includes_base boolean NOT NULL DEFAULT false,
            version bigint NOT NULL DEFAULT 1,
            status varchar(16) NOT NULL DEFAULT 'staging',
            capture_deadline timestamptz NOT NULL,
            staging_expires_at timestamptz NOT NULL,
            created_at timestamptz NOT NULL,
            completed_at timestamptz,
            expires_at timestamptz,
            CONSTRAINT publication_candidate_group_id_key UNIQUE (work_item_id, id),
            CONSTRAINT publication_candidate_status_ck
                CHECK (status IN ('staging', 'complete', 'tombstoned')),
            CONSTRAINT publication_candidate_authority_ck
                CHECK (runtime_epoch >= 1 AND dispatch_generation >= 1 AND version >= 1),
            CONSTRAINT publication_candidate_idempotency_length_ck
                CHECK (char_length(idempotency_key) BETWEEN 1 AND 256),
            CONSTRAINT publication_candidate_digest_ck
                CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
            CONSTRAINT publication_candidate_metadata_ck CHECK (
                char_length(title) <= 256 AND char_length(body) <= 65536 AND
                CASE WHEN jsonb_typeof(changed_paths) = 'array'
                THEN jsonb_array_length(changed_paths) <= 4096 ELSE false END AND
                octet_length(changed_paths::text) <= 1048576),
            CONSTRAINT publication_candidate_completion_ck CHECK (
                (status <> 'complete' OR completed_at IS NOT NULL) AND
                (expires_at IS NULL OR completed_at IS NOT NULL)),
            CONSTRAINT publication_candidate_transfer_ck CHECK (
                (previous_owner_request_id IS NULL) = (owner_transferred_at IS NULL))
        )""",
        "CREATE UNIQUE INDEX publication_candidate_idempotency_key ON "
        "curie.publication_candidates(origin_request_id, idempotency_key) "
        "WHERE status <> 'tombstoned'",
        "CREATE UNIQUE INDEX publication_candidate_one_staging ON "
        "curie.publication_candidates(work_item_id) WHERE status = 'staging'",
        """CREATE TABLE curie.publication_candidate_objects (
            id uuid PRIMARY KEY,
            work_item_id uuid NOT NULL REFERENCES
                curie.publication_candidate_groups(work_item_id) ON DELETE RESTRICT,
            candidate_id uuid,
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
            CONSTRAINT publication_candidate_object_candidate_fkey
                FOREIGN KEY (work_item_id, candidate_id)
                REFERENCES curie.publication_candidates(work_item_id, id) ON DELETE RESTRICT,
            CONSTRAINT publication_candidate_object_candidate_kind_key
                UNIQUE (candidate_id, kind),
            CONSTRAINT publication_candidate_object_kind_ck CHECK (
                kind IN ('base', 'patch') AND ((kind = 'base') = (candidate_id IS NULL))),
            CONSTRAINT publication_candidate_object_descriptor_ck CHECK (
                size_bytes >= 0 AND sha256 ~ '^[0-9a-f]{64}$'),
            CONSTRAINT publication_candidate_object_key_ck CHECK (
                key LIKE 'publication-candidates/%'),
            CONSTRAINT publication_candidate_object_status_ck CHECK (
                status IN ('staging', 'complete', 'tombstoned', 'claimed')),
            CONSTRAINT publication_candidate_object_claim_ck CHECK (
                version >= 0 AND ((status = 'claimed' AND claim_id IS NOT NULL
                AND collector_id IS NOT NULL AND claimed_until IS NOT NULL) OR
                (status <> 'claimed' AND claim_id IS NULL AND collector_id IS NULL
                AND claimed_until IS NULL)))
        )""",
        "CREATE UNIQUE INDEX publication_candidate_object_one_base ON "
        "curie.publication_candidate_objects(work_item_id) "
        "WHERE kind = 'base' AND status IN ('staging', 'complete')",
        "CREATE INDEX publication_candidate_object_group ON "
        "curie.publication_candidate_objects(work_item_id)",
        """CREATE TABLE curie.publication_candidate_readers (
            id uuid PRIMARY KEY,
            work_item_id uuid NOT NULL REFERENCES
                curie.publication_candidate_groups(work_item_id) ON DELETE RESTRICT,
            candidate_id uuid NOT NULL,
            holder_id varchar(256) NOT NULL,
            expires_at timestamptz NOT NULL,
            CONSTRAINT publication_candidate_reader_candidate_fkey
                FOREIGN KEY (work_item_id, candidate_id)
                REFERENCES curie.publication_candidates(work_item_id, id) ON DELETE RESTRICT,
            CONSTRAINT publication_candidate_reader_holder_key
                UNIQUE (work_item_id, candidate_id, holder_id)
        )""",
    )
    for statement in statements:
        op.execute(statement)


def downgrade() -> None:
    if (
        op.get_bind()
        .exec_driver_sql("SELECT EXISTS (SELECT 1 FROM curie.publication_candidate_groups)")
        .scalar_one()
    ):
        raise RuntimeError("publication_candidate_retained_objects_present")
    for table in (
        "publication_candidate_readers",
        "publication_candidate_objects",
        "publication_candidates",
        "publication_candidate_groups",
        "publication_candidate_outcomes",
        "publication_candidate_installation",
    ):
        op.drop_table(table, schema="curie")

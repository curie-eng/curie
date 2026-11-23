"""Durable factory steering and sealed execution input.

Revision ID: 0195
Revises: 0194
"""

from alembic import op

revision = "0195"
down_revision = "0194"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE curie.execution_requests ADD COLUMN steering_sealed_at timestamptz")
    op.execute(
        "CREATE UNIQUE INDEX execution_requests_work_item_id_id_key "
        "ON curie.execution_requests (work_item_id, id)"
    )
    op.execute(
        """
        CREATE TABLE curie.factory_steering_ledger (
            steering_id uuid PRIMARY KEY,
            work_item_id uuid NOT NULL REFERENCES curie.work_items ON DELETE CASCADE,
            source_identity text NOT NULL,
            source_channel text NOT NULL,
            source_root_ts text NOT NULL,
            source_facts jsonb NOT NULL CHECK (jsonb_typeof(source_facts) = 'object'),
            first_delivery_id text NOT NULL,
            source_content_sha256 text NOT NULL CHECK (source_content_sha256 ~ '^[0-9a-f]{64}$'),
            accepted_text_sha256 text NOT NULL CHECK (accepted_text_sha256 ~ '^[0-9a-f]{64}$'),
            accepted_text text NOT NULL CHECK (length(accepted_text) BETWEEN 1 AND 65536),
            state text NOT NULL DEFAULT 'queued',
            execution_request_id uuid,
            objective_embedded boolean NOT NULL DEFAULT false,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            applied_at timestamptz,
            refused_at timestamptz,
            refusal_code text CHECK (refusal_code ~ '^[a-z][a-z0-9_]{0,95}$'),
            FOREIGN KEY (work_item_id, execution_request_id)
                REFERENCES curie.execution_requests (work_item_id, id),
            CHECK (NOT objective_embedded OR execution_request_id IS NOT NULL),
            CHECK ((state = 'queued' AND applied_at IS NULL AND refused_at IS NULL
                    AND refusal_code IS NULL)
                OR (state = 'applied' AND execution_request_id IS NOT NULL
                    AND applied_at IS NOT NULL AND refused_at IS NULL AND refusal_code IS NULL)
                OR (state = 'refused' AND refused_at IS NOT NULL
                    AND applied_at IS NULL AND refusal_code IS NOT NULL))
        )
        """
    )
    op.execute(
        "CREATE INDEX factory_steering_pending ON curie.factory_steering_ledger "
        "(work_item_id, created_at, steering_id) WHERE state = 'queued' "
        "AND execution_request_id IS NULL"
    )
    op.execute(
        "CREATE INDEX factory_steering_assigned ON curie.factory_steering_ledger "
        "(execution_request_id, created_at, steering_id) WHERE execution_request_id IS NOT NULL"
    )
    op.execute(
        "CREATE INDEX factory_steering_reconcile ON curie.factory_steering_ledger "
        "(created_at, steering_id) WHERE state = 'queued'"
    )
    op.execute(
        """
        CREATE FUNCTION curie.check_factory_steering_update() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'INSERT' THEN
            IF NEW.state <> 'queued' THEN
              RAISE EXCEPTION 'invalid_factory_steering_initial_state'
                    USING ERRCODE = 'check_violation';
            END IF;
            RETURN NEW;
          END IF;
          IF OLD.state <> 'queued'
             OR ROW(NEW.steering_id, NEW.work_item_id, NEW.source_identity, NEW.source_channel,
                    NEW.source_root_ts, NEW.source_facts, NEW.first_delivery_id,
                    NEW.source_content_sha256, NEW.accepted_text_sha256, NEW.accepted_text,
                    NEW.created_at) IS DISTINCT FROM
                ROW(OLD.steering_id, OLD.work_item_id, OLD.source_identity, OLD.source_channel,
                    OLD.source_root_ts, OLD.source_facts, OLD.first_delivery_id,
                    OLD.source_content_sha256, OLD.accepted_text_sha256, OLD.accepted_text,
                    OLD.created_at)
             OR (OLD.execution_request_id IS NOT NULL AND
                 ROW(NEW.execution_request_id, NEW.objective_embedded) IS DISTINCT FROM
                 ROW(OLD.execution_request_id, OLD.objective_embedded))
          THEN RAISE EXCEPTION 'invalid_factory_steering_transition'
               USING ERRCODE = 'check_violation';
          END IF;
          RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER check_factory_steering_update BEFORE INSERT OR UPDATE "
        "ON curie.factory_steering_ledger "
        "FOR EACH ROW EXECUTE FUNCTION curie.check_factory_steering_update()"
    )


def downgrade() -> None:
    op.execute(
        "DO $$ BEGIN LOCK TABLE curie.factory_steering_ledger IN ACCESS EXCLUSIVE MODE; "
        "IF EXISTS (SELECT FROM curie.factory_steering_ledger) THEN "
        "RAISE EXCEPTION 'factory_steering_history_present'; END IF; END $$"
    )
    op.execute("DROP TABLE curie.factory_steering_ledger")
    op.execute("DROP FUNCTION curie.check_factory_steering_update()")
    op.execute("DROP INDEX curie.execution_requests_work_item_id_id_key")
    op.execute("ALTER TABLE curie.execution_requests DROP COLUMN steering_sealed_at")

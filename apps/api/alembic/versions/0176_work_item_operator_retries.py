"""Operator retries of a failed factory WorkItem (#5431, ADR 0269).

``work_item_operator_retries`` records each admission an operator asked for
through ``POST /work-items/{id}/retry``. The operator's request UUID is the
primary key, so it is the durable idempotency key: a replay with the same
canonical facts (``facts_sha256``) answers the stored ``result``, and the same
UUID with other facts conflicts. Only admissions are recorded; a refusal or a
dry run writes nothing. The row names the actor kind and its server derived
subject, the bounded reason, the predecessor and the one successor the
admission created, and never a credential.

Expand only: a release without this table never reads or writes it. The
downgrade drops it, which forgets the idempotency records but leaves every
request they name as it is.

Revision ID: 0176
Revises: 0175
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0176"
down_revision: str = "0175"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"


def upgrade() -> None:
    op.execute(
        f"""
        CREATE TABLE {S}.work_item_operator_retries (
          request_id uuid PRIMARY KEY,
          work_item_id uuid NOT NULL
            REFERENCES {S}.work_items (id) ON DELETE CASCADE,
          actor_kind text NOT NULL,
          actor_subject text,
          reason text NOT NULL,
          expected_version integer NOT NULL,
          facts_sha256 text NOT NULL,
          disposition text NOT NULL,
          predecessor_request_id uuid NOT NULL
            REFERENCES {S}.execution_requests (id) ON DELETE CASCADE,
          successor_request_id uuid NOT NULL
            REFERENCES {S}.execution_requests (id) ON DELETE CASCADE,
          result jsonb NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT work_item_operator_retries_successor_key UNIQUE (successor_request_id),
          CONSTRAINT work_item_operator_retries_actor_kind_ck CHECK (
            actor_kind IN ('platform_key', 'console_session')),
          CONSTRAINT work_item_operator_retries_reason_ck CHECK (
            char_length(reason) BETWEEN 1 AND 500),
          CONSTRAINT work_item_operator_retries_disposition_ck CHECK (
            disposition IN ('implementation', 'publication_recovery')),
          CONSTRAINT work_item_operator_retries_expected_version_ck CHECK (
            expected_version >= 1),
          CONSTRAINT work_item_operator_retries_facts_sha_ck CHECK (
            facts_sha256 ~ '^[0-9a-f]{{64}}$'),
          CONSTRAINT work_item_operator_retries_result_ck CHECK (
            jsonb_typeof(result) = 'object')
        )
        """
    )
    op.execute(
        f"CREATE INDEX ix_work_item_operator_retries_work_item_id "
        f"ON {S}.work_item_operator_retries (work_item_id)"
    )


def downgrade() -> None:
    op.execute(f"DROP TABLE {S}.work_item_operator_retries")

"""Factory relabel and bounded cancel settle (#3072).

Adds ``execution_requests.cancellation_requested_at``, the anchor for the
control-plane settle timeout. ``updated_at`` moves on heartbeats and terminate
publishes, so it cannot be the anchor. Existing ``cancellation_requested``
rows are backfilled from ``updated_at``.

Relaxes the sticky WorkItem cancellation trigger: a relabel may clear
``cancelled_at`` back to NULL, but a set value still cannot be rewritten to a
different timestamp.

Adds the ``work_items.readmit_*`` columns: a relabel that arrived while a
request was still running, admitted once that request reaches a terminus.

Adds ``execution_requests.teardown_unconfirmed_at``: set when the control
plane force-settles a cancellation with no worker teardown receipt. The
reconciler keeps publishing terminate wakes for such a row, and a worker may
still claim and record its teardown, until the flag clears.

Revision ID: 0054
Revises: 0053
Create Date: 2026-09-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0054"
down_revision: str | None = "0053"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"


def _work_items_trigger(cancellation_guard: str) -> str:
    return f"""
        CREATE OR REPLACE FUNCTION {SCHEMA}.enforce_work_items_update_invariants()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id
               OR NEW.github_repository_id IS DISTINCT FROM OLD.github_repository_id
               OR NEW.github_issue_number IS DISTINCT FROM OLD.github_issue_number
               OR NEW.github_installation_id IS DISTINCT FROM OLD.github_installation_id
               OR NEW.agent_id IS DISTINCT FROM OLD.agent_id
               OR NEW.repo_full_name IS DISTINCT FROM OLD.repo_full_name
               OR NEW.conversation_id IS DISTINCT FROM OLD.conversation_id
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'work item identity is immutable';
            END IF;

            IF {cancellation_guard} THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'work item cancellation is sticky';
            END IF;

            IF OLD.publication_lineage_id IS NOT NULL
               AND NEW.publication_lineage_id IS DISTINCT FROM OLD.publication_lineage_id THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'work item publication lineage is write once';
            END IF;

            RETURN NEW;
        END;
        $$
        """


# A relabel reopens the WorkItem. Only a clear back to NULL is allowed.
_RELABEL_GUARD = (
    "OLD.cancelled_at IS NOT NULL AND NEW.cancelled_at IS NOT NULL\n"
    "               AND NEW.cancelled_at IS DISTINCT FROM OLD.cancelled_at"
)
_STICKY_GUARD = (
    "OLD.cancelled_at IS NOT NULL\n"
    "               AND NEW.cancelled_at IS DISTINCT FROM OLD.cancelled_at"
)


def upgrade() -> None:
    op.add_column(
        "execution_requests",
        sa.Column("cancellation_requested_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.execute(
        f"""
        UPDATE {SCHEMA}.execution_requests
        SET cancellation_requested_at = updated_at
        WHERE status = 'cancellation_requested'
        """
    )
    op.add_column(
        "work_items",
        sa.Column("readmit_request_id", postgresql.UUID(as_uuid=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "work_items",
        sa.Column("readmit_requester", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "work_items",
        sa.Column("readmit_objective", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "work_items_readmit_ck",
        "work_items",
        "(readmit_request_id IS NULL AND readmit_requester IS NULL "
        "AND readmit_objective IS NULL) OR "
        "(readmit_request_id IS NOT NULL AND readmit_requester IS NOT NULL "
        "AND readmit_objective IS NOT NULL)",
        schema=SCHEMA,
    )
    op.add_column(
        "execution_requests",
        sa.Column("teardown_unconfirmed_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "execution_requests_teardown_unconfirmed_ck",
        "execution_requests",
        "teardown_unconfirmed_at IS NULL OR status = 'cancelled'",
        schema=SCHEMA,
    )
    op.execute(_work_items_trigger(_RELABEL_GUARD))


def downgrade() -> None:
    op.drop_constraint(
        "execution_requests_teardown_unconfirmed_ck", "execution_requests", schema=SCHEMA
    )
    op.drop_column("execution_requests", "teardown_unconfirmed_at", schema=SCHEMA)
    op.execute(_work_items_trigger(_STICKY_GUARD))
    op.drop_constraint("work_items_readmit_ck", "work_items", schema=SCHEMA)
    op.drop_column("work_items", "readmit_objective", schema=SCHEMA)
    op.drop_column("work_items", "readmit_requester", schema=SCHEMA)
    op.drop_column("work_items", "readmit_request_id", schema=SCHEMA)
    op.drop_column("execution_requests", "cancellation_requested_at", schema=SCHEMA)

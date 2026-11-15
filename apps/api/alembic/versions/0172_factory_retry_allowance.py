"""Shared automatic retry allowance for factory executions (#5424, ADR 0269).

A WorkItem owns two lifetime automatic retry slots. ``work_items`` gains the
counter ``automatic_retries_used``, backfilled from the surviving owner loss
successors, and a trigger that refuses any update that lowers it, so a slot once
spent is never returned. ``execution_requests`` gains the lineage of an
automatic successor (``retry_of_request_id`` and ``retry_origin_request_id``),
the typed condition a scheduled successor waits on (``blocked_on``), the safety
fact and optional provider condition a finish reported (``retry_safety`` and the
three ``retry_condition_*`` columns), and a unique index that admits at most one
successor per predecessor. No foreign key is added on the lineage columns:
requests are deleted only with their WorkItem.

``execution_requests_state_shape_ck`` admits the new terminal cause
``retry_condition_expired`` for an unstarted expired request only when it was
blocked. ``capacity_wait_expired`` stays legal whether or not the request was
blocked, so a previous version replica that expires a blocked request still
writes a legal row. The upgrade refuses unless the live check is the 0160 one,
compared by the stable form this server prints for it. The downgrade refuses before any
DDL while retry state exists, because dropping it would let a later upgrade
count fewer used slots than were spent, then restores the 0160 check.

Revision ID: 0172
Revises: 0171
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0172"
down_revision: str = "0171"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"

# A bounded wait for the tables the downgrade guard reads, so a busy table
# refuses the downgrade rather than blocking writers behind it indefinitely.
LOCK_TIMEOUT_MS = 15000

SHAPE = "execution_requests_state_shape_ck"
REQUEST_COLUMNS = (
    "retry_of_request_id",
    "retry_origin_request_id",
    "blocked_on",
    "retry_safety",
    "retry_condition_reason",
    "retry_condition_retry_at",
    "retry_condition_scope_key",
)
REQUEST_CHECKS = (
    "execution_requests_retry_lineage_ck",
    "execution_requests_blocked_unstarted_ck",
    "execution_requests_retry_safety_ck",
    "execution_requests_retry_condition_ck",
)

# Frozen: the 0160 definition, which no revision after 0160 changed. The
# upgrade recognizes the live check as this body and the downgrade restores it.
PRIOR_SHAPE = (
    "((status = 'failed' AND started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_at IS NOT NULL AND terminal_cause = 'start_failed' "
    "AND termination_observation IS NULL) "
    "OR (status = 'queued' AND wait_deadline IS NULL AND started_at IS NULL "
    "AND execution_deadline IS NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NULL AND termination_observation IS NULL) "
    "OR (status = 'cancelled' AND wait_deadline IS NULL AND started_at IS NULL "
    "AND execution_deadline IS NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted', 'lineage_closed') "
    "AND termination_observation IS NULL) "
    "OR (status = 'waiting' AND started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_at IS NULL AND terminal_cause IS NULL "
    "AND termination_observation IS NULL) "
    "OR (status = 'running' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NULL AND termination_observation IS NULL) "
    "OR (status = 'cancellation_requested' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted', "
    "'execution_deadline', 'owner_lost', 'enrollment_fenced') "
    "AND termination_observation IS NULL) "
    "OR (status = 'completed' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL AND terminal_cause = 'completed') "
    "OR (status = 'failed' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL "
    "AND (terminal_cause NOT IN ('owner_lost', 'enrollment_fenced') "
    "OR termination_observation IS NOT NULL)) "
    "OR (status = 'expired' AND terminal_at IS NOT NULL AND "
    "((started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_cause IS NOT NULL AND terminal_cause = 'capacity_wait_expired' "
    "AND termination_observation IS NULL) OR "
    "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
    "AND terminal_cause IS NOT NULL AND terminal_cause = 'execution_deadline' "
    "AND termination_observation IS NOT NULL))) "
    "OR (status = 'cancelled' AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted') AND "
    "((started_at IS NULL AND execution_deadline IS NULL "
    "AND termination_observation IS NULL) OR "
    "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
    "AND termination_observation IS NOT NULL)))) IS TRUE"
)

# Frozen: the 0172 definition, copied in ``models.ExecutionRequest``. Only the
# unstarted expired branch differs from 0160.
STATE_SHAPE = (
    "((status = 'failed' AND started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_at IS NOT NULL AND terminal_cause = 'start_failed' "
    "AND termination_observation IS NULL) "
    "OR (status = 'queued' AND wait_deadline IS NULL AND started_at IS NULL "
    "AND execution_deadline IS NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NULL AND termination_observation IS NULL) "
    "OR (status = 'cancelled' AND wait_deadline IS NULL AND started_at IS NULL "
    "AND execution_deadline IS NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted', 'lineage_closed') "
    "AND termination_observation IS NULL) "
    "OR (status = 'waiting' AND started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_at IS NULL AND terminal_cause IS NULL "
    "AND termination_observation IS NULL) "
    "OR (status = 'running' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NULL AND termination_observation IS NULL) "
    "OR (status = 'cancellation_requested' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NULL "
    "AND terminal_cause IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted', "
    "'execution_deadline', 'owner_lost', 'enrollment_fenced') "
    "AND termination_observation IS NULL) "
    "OR (status = 'completed' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL AND terminal_cause = 'completed') "
    "OR (status = 'failed' AND started_at IS NOT NULL "
    "AND execution_deadline IS NOT NULL AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL "
    "AND (terminal_cause NOT IN ('owner_lost', 'enrollment_fenced') "
    "OR termination_observation IS NOT NULL)) "
    # 0172: a blocked unstarted request that ran out of wait expires with its
    # own cause; capacity_wait_expired stays legal with or without a block.
    "OR (status = 'expired' AND terminal_at IS NOT NULL AND "
    "((started_at IS NULL AND execution_deadline IS NULL "
    "AND terminal_cause IS NOT NULL "
    "AND (terminal_cause = 'capacity_wait_expired' "
    "OR (terminal_cause = 'retry_condition_expired' AND blocked_on IS NOT NULL)) "
    "AND termination_observation IS NULL) OR "
    "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
    "AND terminal_cause IS NOT NULL AND terminal_cause = 'execution_deadline' "
    "AND termination_observation IS NOT NULL))) "
    "OR (status = 'cancelled' AND terminal_at IS NOT NULL "
    "AND terminal_cause IS NOT NULL "
    "AND terminal_cause IN ('issue_cancelled', 'issue_deleted') AND "
    "((started_at IS NULL AND execution_deadline IS NULL "
    "AND termination_observation IS NULL) OR "
    "(started_at IS NOT NULL AND execution_deadline IS NOT NULL "
    "AND termination_observation IS NOT NULL)))) IS TRUE"
)


def _definition(name: str) -> str | None:
    shape = (
        op.get_bind()
        .exec_driver_sql(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            f"WHERE conrelid = '{S}.execution_requests'::regclass AND conname = '{name}'"
        )
        .scalar()
    )
    if not isinstance(shape, str) or not shape.startswith("CHECK (") or not shape.endswith(")"):
        return None
    return shape[len("CHECK (") : -1]


def _printed(bodies: Sequence[str]) -> list[str]:
    """How this server prints each body as a CHECK on the request columns.

    Comparing printed forms keeps the recognition independent of the server
    version's formatting. The probe table is temporary and holds no rows.
    """

    bind = op.get_bind()
    bind.exec_driver_sql(
        f"CREATE TEMPORARY TABLE curie_0172_shape_probe (LIKE {S}.execution_requests)"
    )
    try:
        printed = []
        for index, body in enumerate(bodies):
            bind.exec_driver_sql(
                f"ALTER TABLE curie_0172_shape_probe ADD CONSTRAINT probe_{index} CHECK ({body})"
            )
            printed.append(
                bind.exec_driver_sql(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                    "WHERE conrelid = 'curie_0172_shape_probe'::regclass "
                    f"AND conname = 'probe_{index}'"
                ).scalar_one()
            )
        return printed
    finally:
        bind.exec_driver_sql("DROP TABLE curie_0172_shape_probe")


def _canonical(body: str) -> str:
    """The fixed point of printing ``body`` as a CHECK on the request columns.

    One server pass does not always reproduce its own output: a freshly created
    check and a dumped and restored one print differently. Printing until the
    text stops changing makes both spellings of one shape compare equal.
    """

    text = body
    for _ in range(3):
        printed = _printed([text])[0]
        if not printed.startswith("CHECK (") or not printed.endswith(")"):
            break
        printed = printed[len("CHECK (") : -1]
        if printed == text:
            return text
        text = printed
    raise RuntimeError("execution request state shape is not the 0160 shape")


def _replace(name: str, body: str) -> None:
    op.drop_constraint(name, "execution_requests", schema=S, type_="check")
    op.create_check_constraint(name, "execution_requests", body, schema=S)


def upgrade() -> None:
    current = _definition(SHAPE)
    if current is None or _canonical(current) != _canonical(PRIOR_SHAPE):
        raise RuntimeError("execution request state shape is not the 0160 shape")

    op.execute(
        f"""
        ALTER TABLE {S}.work_items
          ADD COLUMN automatic_retries_used integer NOT NULL DEFAULT 0
        """
    )
    op.execute(
        f"""
        ALTER TABLE {S}.work_items
          ADD CONSTRAINT work_items_automatic_retries_used_ck
          CHECK (automatic_retries_used >= 0)
        """
    )
    # Surviving owner loss successors are the slots already spent. The value
    # is never clamped, and version and updated_at are not touched.
    op.execute(
        f"""
        UPDATE {S}.work_items AS w
           SET automatic_retries_used = s.used
          FROM (SELECT work_item_id, count(*)::integer AS used
                  FROM {S}.execution_requests
                 WHERE owner_lost_retry
                 GROUP BY work_item_id) AS s
         WHERE w.id = s.work_item_id
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {S}.enforce_work_items_retry_usage_monotonic()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.automatic_retries_used < OLD.automatic_retries_used THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'work item automatic retry usage never decreases';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER work_items_retry_usage_monotonic
        BEFORE UPDATE OF automatic_retries_used ON {S}.work_items
        FOR EACH ROW
        EXECUTE FUNCTION {S}.enforce_work_items_retry_usage_monotonic()
        """
    )

    op.execute(
        f"""
        ALTER TABLE {S}.execution_requests
          ADD COLUMN retry_of_request_id uuid,
          ADD COLUMN retry_origin_request_id uuid,
          ADD COLUMN blocked_on text,
          ADD COLUMN retry_safety text,
          ADD COLUMN retry_condition_reason text,
          ADD COLUMN retry_condition_retry_at timestamptz,
          ADD COLUMN retry_condition_scope_key text
        """
    )
    op.execute(
        f"""
        ALTER TABLE {S}.execution_requests
          ADD CONSTRAINT execution_requests_retry_lineage_ck CHECK (
            (retry_of_request_id IS NULL) = (retry_origin_request_id IS NULL)
            AND (retry_of_request_id IS NULL OR retry_of_request_id <> id)),
          ADD CONSTRAINT execution_requests_blocked_unstarted_ck CHECK (
            blocked_on IS NULL OR (blocked_on <> '' AND started_at IS NULL)),
          ADD CONSTRAINT execution_requests_retry_safety_ck CHECK (
            retry_safety IS NULL
            OR (retry_safety IN ('no_effects', 'flagged', 'uncertain')
                AND terminal_at IS NOT NULL)),
          ADD CONSTRAINT execution_requests_retry_condition_ck CHECK (
            retry_condition_reason IS NOT NULL
            OR (retry_condition_retry_at IS NULL AND retry_condition_scope_key IS NULL))
        """
    )
    op.execute(
        f"""
        CREATE UNIQUE INDEX uq_execution_requests_retry_of
            ON {S}.execution_requests (retry_of_request_id)
         WHERE retry_of_request_id IS NOT NULL
        """
    )
    _replace(SHAPE, STATE_SHAPE)


def downgrade() -> None:
    # Held from the check to the restore, so no retry state lands in between.
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    op.execute(
        sa.text(f"LOCK TABLE {S}.execution_requests, {S}.work_items IN ACCESS EXCLUSIVE MODE")
    )
    present = (
        op.get_bind()
        .exec_driver_sql(
            f"SELECT EXISTS (SELECT 1 FROM {S}.execution_requests WHERE "
            "retry_of_request_id IS NOT NULL OR blocked_on IS NOT NULL "
            "OR terminal_cause = 'retry_condition_expired')"
        )
        .scalar_one()
    )
    if present:
        raise RuntimeError("factory_retry_state_present")
    _replace(SHAPE, PRIOR_SHAPE)
    op.execute(f"DROP INDEX {S}.uq_execution_requests_retry_of")
    for name in REQUEST_CHECKS:
        op.drop_constraint(name, "execution_requests", schema=S, type_="check")
    for column in REQUEST_COLUMNS:
        op.drop_column("execution_requests", column, schema=S)
    op.execute(f"DROP TRIGGER work_items_retry_usage_monotonic ON {S}.work_items")
    op.execute(f"DROP FUNCTION {S}.enforce_work_items_retry_usage_monotonic()")
    op.drop_constraint(
        "work_items_automatic_retries_used_ck", "work_items", schema=S, type_="check"
    )
    op.drop_column("work_items", "automatic_retries_used", schema=S)

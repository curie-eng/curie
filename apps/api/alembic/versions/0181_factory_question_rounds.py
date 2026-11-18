"""Factory question rounds and answer receipts (#5785, one-ticket ADR).

1. ``factory_question_rounds``: one row per WorkItem holding the number of
   question rounds whose comment was posted (0 to 3), the round's state, the
   asking request, the tracker comment id, a digest of the comment the
   platform wrote, the answer fence ``answer_after`` and the newest request an
   answer created. No question or answer text.
2. ``factory_answer_receipts``: one receipt per answer comment, written by the
   answer core (#5786), keyed by WorkItem and the tracker's comment id.
3. ``factory_terminal_notices_applied_label_ck`` also accepts
   ``curie-factory:questions``.
4. ``execution_requests_state_shape_ck`` lets a started ``completed`` request
   end with cause ``questions_asked`` as well as ``completed``: a request that
   posted questions and then ends without a pull request completes with that
   cause. Every other branch is the 0172 shape. The upgrade refuses unless the
   live check is the 0172 one, compared by the stable form this server prints.

Expand only: a release without this revision never reads the new tables and
never writes the new label or cause. The downgrade refuses while a status
comment row records the new label or a request records the new cause, either
of which the restored checks would reject, then restores both checks and drops
both tables.

Revision ID: 0181
Revises: 0180
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0181"
down_revision: str = "0180"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

S = "curie"
SHAPE = "execution_requests_state_shape_ck"
LOCK_TIMEOUT_MS = 15000
ROUNDS = "factory_question_rounds"
RECEIPTS = "factory_answer_receipts"
NOTICES = "factory_terminal_notices"

_LABELS_0167 = (
    "'', 'curie:queued', 'curie:running', 'curie:pr-open', 'curie:needs-human', "
    "'curie-factory:queued', 'curie-factory:running', 'curie-factory:pr-open', "
    "'curie-factory:needs-human', 'curie-factory:triaged'"
)
_LABELS = f"{_LABELS_0167}, 'curie-factory:questions'"
_STATES = "'posting', 'outstanding', 'answered', 'cleared', 'exhausted'"
_OUTCOMES = (
    "'admitted', 'coalesced', 'queued_follow_up', 'not_outstanding', "
    "'not_after_question', 'work_item_cancelled'"
)


# Frozen: the 0172 definition, which no revision after 0172 changed. The
# upgrade recognizes the live check as this body and the downgrade restores it.
_SHAPE_0172 = (
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

_COMPLETED_0172 = "AND terminal_cause IS NOT NULL AND terminal_cause = 'completed') "
_COMPLETED = (
    "AND terminal_cause IS NOT NULL AND terminal_cause IN ('completed', 'questions_asked')) "
)
assert _SHAPE_0172.count(_COMPLETED_0172) == 1
# The 0181 definition, copied in ``models.ExecutionRequest``: only the
# started completed branch differs from 0172.
STATE_SHAPE = _SHAPE_0172.replace(_COMPLETED_0172, _COMPLETED)


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
        f"CREATE TEMPORARY TABLE curie_0181_shape_probe (LIKE {S}.execution_requests)"
    )
    try:
        printed = []
        for index, body in enumerate(bodies):
            bind.exec_driver_sql(
                f"ALTER TABLE curie_0181_shape_probe ADD CONSTRAINT probe_{index} CHECK ({body})"
            )
            printed.append(
                bind.exec_driver_sql(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                    "WHERE conrelid = 'curie_0181_shape_probe'::regclass "
                    f"AND conname = 'probe_{index}'"
                ).scalar_one()
            )
        return printed
    finally:
        bind.exec_driver_sql("DROP TABLE curie_0181_shape_probe")


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
    raise RuntimeError("execution request state shape is not the 0172 shape")


def _replace_shape(body: str) -> None:
    op.drop_constraint(SHAPE, "execution_requests", schema=S, type_="check")
    op.create_check_constraint(SHAPE, "execution_requests", body, schema=S)


def _replace_label_check(labels: str) -> None:
    name = f"{NOTICES}_applied_label_ck"
    op.drop_constraint(name, NOTICES, schema=S)
    op.create_check_constraint(
        name, NOTICES, f"applied_label IS NULL OR applied_label IN ({labels})", schema=S
    )


def upgrade() -> None:
    current = _definition(SHAPE)
    if current is None or _canonical(current) != _canonical(_SHAPE_0172):
        raise RuntimeError("execution request state shape is not the 0172 shape")
    _replace_shape(STATE_SHAPE)
    op.create_table(
        ROUNDS,
        sa.Column(
            "work_item_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{S}.work_items.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("rounds_used", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("asking_request_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("comment_id", sa.Text(), nullable=True),
        sa.Column("body_sha256", sa.Text(), nullable=False),
        sa.Column("answer_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pending_request_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint("rounds_used BETWEEN 0 AND 3", name=f"{ROUNDS}_rounds_used_ck"),
        sa.CheckConstraint(f"state IN ({_STATES})", name=f"{ROUNDS}_state_ck"),
        sa.CheckConstraint(
            "rounds_used >= 1 OR state IN ('posting', 'cleared')",
            name=f"{ROUNDS}_posted_ck",
        ),
        sa.CheckConstraint("body_sha256 ~ '^[0-9a-f]{64}$'", name=f"{ROUNDS}_body_sha256_ck"),
        schema=S,
    )
    op.create_table(
        RECEIPTS,
        sa.Column(
            "work_item_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{S}.work_items.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("comment_key", sa.Text(), primary_key=True),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("request_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.CheckConstraint(f"outcome IN ({_OUTCOMES})", name=f"{RECEIPTS}_outcome_ck"),
        schema=S,
    )
    _replace_label_check(_LABELS)


def downgrade() -> None:
    # Held from the check to the restore, so no new label or cause lands between.
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    op.execute(
        sa.text(f"LOCK TABLE {S}.execution_requests, {S}.{NOTICES} IN ACCESS EXCLUSIVE MODE")
    )
    bind = op.get_bind()
    present = bind.scalar(
        sa.text(
            f"SELECT EXISTS (SELECT 1 FROM {S}.{NOTICES} "
            "WHERE applied_label = 'curie-factory:questions')"
        )
    )
    if present:
        raise RuntimeError("factory_question_label_present")
    ended = bind.scalar(
        sa.text(
            f"SELECT EXISTS (SELECT 1 FROM {S}.execution_requests "
            "WHERE terminal_cause = 'questions_asked')"
        )
    )
    if ended:
        raise RuntimeError("factory_question_ending_present")
    _replace_label_check(_LABELS_0167)
    _replace_shape(_SHAPE_0172)
    op.drop_table(RECEIPTS, schema=S)
    op.drop_table(ROUNDS, schema=S)

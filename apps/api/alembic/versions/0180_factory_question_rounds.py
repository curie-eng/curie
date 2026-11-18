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

Expand only: a release without this revision never reads the new tables and
never writes the new label. The downgrade refuses while a status comment row
records the new label, which the restored CHECK would reject, then restores
the CHECK and drops both tables.

Revision ID: 0180
Revises: 0179
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0180"
down_revision: str = "0179"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

S = "curie"
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


def _replace_label_check(labels: str) -> None:
    name = f"{NOTICES}_applied_label_ck"
    op.drop_constraint(name, NOTICES, schema=S)
    op.create_check_constraint(
        name, NOTICES, f"applied_label IS NULL OR applied_label IN ({labels})", schema=S
    )


def upgrade() -> None:
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
    present = op.get_bind().scalar(
        sa.text(
            f"SELECT EXISTS (SELECT 1 FROM {S}.{NOTICES} "
            "WHERE applied_label = 'curie-factory:questions')"
        )
    )
    if present:
        raise RuntimeError("factory_question_label_present")
    _replace_label_check(_LABELS_0167)
    op.drop_table(RECEIPTS, schema=S)
    op.drop_table(ROUNDS, schema=S)

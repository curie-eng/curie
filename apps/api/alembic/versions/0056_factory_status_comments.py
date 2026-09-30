"""Factory terminal notices become live status comments (#3077).

Keeps the table name ``factory_terminal_notices`` (the ORM class is
``FactoryStatusComment``) so this stays an ``expand`` revision: one
bot-authored comment per execution request, inserted at admission and edited
in place as the run progresses. ``terminal_cause`` becomes nullable because the
row now exists before the terminus. New columns carry the card capability
token, which comment list the comment lives in, the last rendered digest, the
finalization stamp, the issue title, the applied ``curie:*`` label, and the
bundle's phase declaration plus the latest activity counters.

Application N-1 keeps working: ``card_token`` and ``updated_at`` carry server
defaults, every new CHECK accepts a row N-1 writes (a posted comment with no
``comment_list``), and N-1's ``ix_factory_terminal_notices_pending`` index is
kept beside the new unfinalized index.

Backfill keeps every comment already posted final (``finalized_at =
posted_at``), so no pre-existing terminal comment is ever re-edited, and sets
``applied_label = ''`` so old WorkItems see no label churn.

Adds ``execution_request_phase_reports``, one row per ``report_progress`` call.

Downgrade refuses while any row has no terminal cause: the old schema cannot
hold a comment that is not yet terminal.

Revision ID: 0056
Revises: 0055
Create Date: 2026-09-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0056"
down_revision: str | None = "0055"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "factory_terminal_notices"
P = TABLE
_LABELS = "('', 'curie:queued', 'curie:running', 'curie:pr-open', 'curie:needs-human')"
_TOKEN_DEFAULT = (
    "replace(gen_random_uuid()::text, '-', '') || replace(gen_random_uuid()::text, '-', '')"
)
_NEW_CHECKS = (
    "applied_label_ck",
    "subject_title_ck",
    "finalized_ck",
    "comment_list_pair_ck",
    "comment_list_ck",
)


def upgrade() -> None:
    op.drop_constraint(f"{P}_cause_ck", TABLE, schema=SCHEMA)
    op.alter_column(TABLE, "terminal_cause", nullable=True, schema=SCHEMA)
    op.create_check_constraint(
        f"{P}_cause_ck",
        TABLE,
        "terminal_cause IS NULL OR length(btrim(terminal_cause)) > 0",
        schema=SCHEMA,
    )

    op.add_column(
        TABLE,
        sa.Column(
            "card_token",
            sa.Text(),
            server_default=sa.text(_TOKEN_DEFAULT),
            nullable=False,
        ),
        schema=SCHEMA,
    )
    op.add_column(TABLE, sa.Column("comment_list", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column(TABLE, sa.Column("rendered_digest", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column(
        TABLE,
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(TABLE, sa.Column("subject_title", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column(TABLE, sa.Column("applied_label", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column(
        TABLE,
        sa.Column("declaration", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        TABLE,
        sa.Column("activity", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        TABLE,
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        schema=SCHEMA,
    )

    op.execute(f"UPDATE {SCHEMA}.{TABLE} SET applied_label = ''")
    op.execute(
        f"""
        UPDATE {SCHEMA}.{TABLE} AS c
        SET finalized_at = c.posted_at,
            comment_list = CASE
                WHEN r.objective LIKE '%#discussion\\_r%' THEN 'review'
                ELSE 'issue'
            END
        FROM {SCHEMA}.execution_requests AS r
        WHERE r.id = c.execution_request_id AND c.posted_at IS NOT NULL
        """
    )
    op.create_unique_constraint(f"{P}_card_token_key", TABLE, ["card_token"], schema=SCHEMA)
    op.create_check_constraint(
        f"{P}_comment_list_ck",
        TABLE,
        "comment_list IS NULL OR comment_list IN ('issue', 'review')",
        schema=SCHEMA,
    )
    # N-1 posts a comment without naming its list, so only the reverse holds.
    op.create_check_constraint(
        f"{P}_comment_list_pair_ck",
        TABLE,
        "comment_list IS NULL OR comment_id IS NOT NULL",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        f"{P}_finalized_ck",
        TABLE,
        "finalized_at IS NULL OR posted_at IS NOT NULL",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        f"{P}_subject_title_ck",
        TABLE,
        "subject_title IS NULL OR length(subject_title) <= 256",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        f"{P}_applied_label_ck",
        TABLE,
        f"applied_label IS NULL OR applied_label IN {_LABELS}",
        schema=SCHEMA,
    )
    # ix_factory_terminal_notices_pending stays for N-1's delivery scan.
    op.create_index(
        f"ix_{P}_unfinalized",
        TABLE,
        ["created_at"],
        schema=SCHEMA,
        postgresql_where=sa.text("finalized_at IS NULL AND refused_at IS NULL"),
    )

    op.create_table(
        "execution_request_phase_reports",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column(
            "execution_request_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.execution_requests.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("phase", sa.Text(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("loop_round", sa.Integer(), nullable=True),
        sa.Column(
            "reported_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "phase ~ '^[a-z][a-z0-9_]{0,63}$'",
            name="execution_request_phase_reports_phase_ck",
        ),
        sa.CheckConstraint(
            "note IS NULL OR length(note) BETWEEN 1 AND 280",
            name="execution_request_phase_reports_note_ck",
        ),
        sa.CheckConstraint(
            "loop_round IS NULL OR loop_round BETWEEN 1 AND 5",
            name="execution_request_phase_reports_round_ck",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_execution_request_phase_reports_request",
        "execution_request_phase_reports",
        ["execution_request_id", "id"],
        schema=SCHEMA,
    )



def downgrade() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {SCHEMA}.{TABLE} WHERE terminal_cause IS NULL) THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = '0056 downgrade refused: a status comment has no '
                              'terminal cause, which 0055 cannot hold';
            END IF;
        END
        $$
        """
    )
    op.drop_index(
        "ix_execution_request_phase_reports_request",
        table_name="execution_request_phase_reports",
        schema=SCHEMA,
    )
    op.drop_table("execution_request_phase_reports", schema=SCHEMA)

    op.drop_index(f"ix_{P}_unfinalized", table_name=TABLE, schema=SCHEMA)
    for name in (*_NEW_CHECKS, "card_token_key"):
        op.drop_constraint(f"{P}_{name}", TABLE, schema=SCHEMA)
    for column in (
        "updated_at",
        "activity",
        "declaration",
        "applied_label",
        "subject_title",
        "finalized_at",
        "rendered_digest",
        "comment_list",
        "card_token",
    ):
        op.drop_column(TABLE, column, schema=SCHEMA)
    op.drop_constraint(f"{P}_cause_ck", TABLE, schema=SCHEMA)
    op.alter_column(TABLE, "terminal_cause", nullable=False, schema=SCHEMA)
    op.create_check_constraint(
        f"{P}_cause_ck", TABLE, "length(btrim(terminal_cause)) > 0", schema=SCHEMA
    )

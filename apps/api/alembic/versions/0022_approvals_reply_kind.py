"""approvals carry their own routing half: reply_kind + reply_adapter (#1459)

The resume turn is rebuilt from the approval row, possibly days after it was
raised and possibly after an operator re-pointed the binding. So the channel KIND
has to be a recorded fact about the ORIGINAL turn, not something re-derived from
`agent_channels` at resume time -- re-deriving it is the silent misroute ADR-0096
phase 2 exists to close, at its least observable point.

`reply_adapter` rides the same revision: the durable twin of
`ReplyHandle.adapter`, plain nullable with NO backfill HERE. Slack legitimately
has no adapter, so NULL is the correct value for a Slack-routed row; a NON-Slack
row is a different matter, because an approval reconstructed with a kind but no
adapter resumes to the HTTP sink with no credential selector and fails closed
mid-resume. That backfill-or-refuse cannot live in this revision: the column it
would read, `agent_channels.adapter`, does not exist until 0024. It is therefore
judged in **0024**, immediately after the route columns are created: since that
revision creates them NULL, no non-Slack approval has adapter provenance, and
0024 REFUSES and names those rows instead of leaving them unresumable. The two
halves of one rule are split by column order alone.

**The backfill is BY PROVENANCE, and refuses rather than guesses.** Each approval
joins `agent_channels` on `approvals.reply_channel = agent_channels.address`. A
row whose address resolves to exactly one binding takes that binding's kind. A row
resolving to zero bindings, or to more than one kind, is UNRECONSTRUCTABLE, and
the migration RAISES on it whatever its status, naming every offending id and its
`reply_channel`. There is deliberately no force-through flag; the operator's next
step is to re-point or remove the offending bindings.

**The refusal is status-BLIND, because no status in this schema is provably
inert.** An earlier revision let a settled (approved/rejected/expired) row take a
fabricated `'slack'` on the theory that it owes no wake. It does owe one:
`crud._RESUMABLE_STATUSES` is exactly `(approved, rejected, expired)`, so
`resumequeue.resume_turn_for` builds a resume turn for all three, and
`crud.reopen_dead_lettered_resume` clears `resumed_at` on a row whose resume was
dead-lettered (#532), putting an already-resumed row back on the reconciler's
work-list. Only `pending` owes nothing YET -- and it is the one status that
certainly will. So a fabricated kind on a settled row does not avoid the
misroute, it defers it to a wake nobody is watching for, which is strictly worse
than an abort the operator can see. `'slack'` is written when, and only when, a
row's own address resolves to a slack binding: earned provenance, never a
default.

**Stated rather than papered over: this preflight decides on the CURRENT binding,
and the current binding is mutable.** `crud.update_agent_binding` rewrites kind and
address in place, so an approval raised when the address was bound to `email` and
since re-pointed to `webhook` preflights as `webhook`. There is no historical
record to consult -- that gap is precisely what `reply_kind` closes going forward,
and it cannot be closed retroactively for rows written before the column existed.
Detectable ambiguity aborts; undetectable staleness is documented residual risk,
mitigated by the cutover's operator-confirmation step, which is why every pending
backfill is logged with its id, address and chosen kind.

`downgrade` REFUSES in two directions, because dropping the column destroys
durable routing identity for exactly the rows that need it:

1. any approval carrying a non-`'slack'` kind -- obviously unrepresentable; and
2. any `'slack'` approval whose `reply_channel` is CURRENTLY bound to a different
   kind. After the drop a resume would re-derive the kind from the binding and get
   the wrong one: the same silent misroute in the opposite direction.

The add-nullable / backfill / set-not-null shape is migration 0021's own, for the
reason it states: adding a NOT NULL column to a populated table has no value to
put in the existing rows. No `server_default` is left behind, deliberately -- an
old API pod inserting during the window must fail loudly rather than fabricate a
kind.

Revision ID: 0022
Revises: 0021a
Create Date: 2026-08-13
"""

import logging
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from curie_api.migration_fence import (
    AUDIT_COLUMNS_AT_0013,
    UnreconstructableRow,
    fence_identity_tables,
    honor_declarations,
    load_declarations,
    report_and_guidance,
)

revision: str = "0022"
down_revision: str | None = "0021a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "approvals"
CHANNELS = "agent_channels"

# The kind whose absence the downgrade checks for, and the only kind a row can
# EARN from its own binding without being unrepresentable afterwards. It is never
# a default here: inventing it for a row with no provenance is the failure this
# migration refuses on, whatever that row's status.
SLACK = "slack"

logger = logging.getLogger("alembic.runtime.migration")


def upgrade() -> None:
    conn = op.get_bind()

    # The identity fence. The preflight, the declaration arm, the backfill and
    # the constraint tightening below all commit as ONE unit behind it, so a
    # binding cannot be re-pointed between the preflight's answer and the
    # backfill that records it. Alembic's per-migration transaction
    # (`env.py:61`) is what releases it. It takes ACCESS EXCLUSIVE -- the
    # strongest mode this revision needs, since the ADD COLUMNs below need it
    # anyway -- so there is no later lock upgrade to deadlock a resolver that
    # read before it wrote. A concurrent writer BLOCKS and then SUCCEEDS: an
    # in-flight turn's approval request is queued, never refused.
    fence_identity_tables(conn)

    # 0. The adapter selector: nullable, no backfill, no constraint.
    op.add_column(TABLE, sa.Column("reply_adapter", sa.String(), nullable=True), schema=SCHEMA)

    # 1. The routing half, nullable for now so existing rows have somewhere to land.
    op.add_column(TABLE, sa.Column("reply_kind", sa.String(), nullable=True), schema=SCHEMA)

    # 2. Preflight. An approval is reconstructable when its address resolves to
    # exactly one KIND -- one kind, not one row: two bindings of the same kind at
    # one address (reachable only out of band) still name a single honest answer.
    unreconstructable = conn.execute(
        sa.text(
            f"""
            SELECT ap.id AS id,
                   ap.reply_channel AS reply_channel,
                   ap.status AS status,
                   count(DISTINCT c.kind) AS kinds
            FROM {SCHEMA}.{TABLE} ap
            LEFT JOIN {SCHEMA}.{CHANNELS} c ON c.address = ap.reply_channel
            GROUP BY ap.id, ap.reply_channel, ap.status
            HAVING count(DISTINCT c.kind) <> 1
            ORDER BY ap.id
            """
        )
    ).all()

    # 2b. The declaration arm. An operator may vouch, per row and by hand, for
    # the identity an approval was RAISED on -- and for EXACTLY the rows above,
    # never for one this preflight can still establish. Each honored declaration
    # appends one audit row, so the bypass is attributed rather than silent, and
    # nothing is deleted. The 0013 column set is passed explicitly:
    # `principal_kind` and `authenticated` do not exist until 0038.
    rows = [
        UnreconstructableRow(
            approval_id=str(row.id),
            reply_channel=row.reply_channel,
            status=row.status,
            reason=(
                "its address resolves to no binding"
                if row.kinds == 0
                else f"its address resolves to {row.kinds} distinct kinds"
            ),
        )
        for row in unreconstructable
    ]
    honored = honor_declarations(
        conn,
        unreconstructable={row.approval_id: row.reason for row in rows},
        declarations=load_declarations(),
        # Nothing is established at this revision: `reply_kind` is created NULL
        # eight statements up, so a declaration here supplies the whole
        # identity. 0024 passes the kinds THIS revision persisted, which is what
        # stops a later revision rewriting an origin established here.
        established_kinds={},
        revision=revision,
        audit_columns=AUDIT_COLUMNS_AT_0013,
    )
    rows = [row for row in rows if row.approval_id not in honored]

    # 3. Refuse on ambiguity, whatever the row's status. A settled row is not
    # inert here: `crud._RESUMABLE_STATUSES` is exactly (approved, rejected,
    # expired), and #532 can re-open an already-resumed row, so every status can
    # still owe a wake. Guessing buys a deferred misroute instead of an abort.
    #
    # The refusal carries the WHOLE disposition -- every offending row's facts
    # and a ready-to-fill declaration document -- because a blocked installation
    # is on a pre-head schema and cannot start the API that would report them.
    if rows:
        detail = "; ".join(
            f"{row.approval_id} (reply_channel {row.reply_channel!r}, "
            f"status {row.status}: {row.reason})"
            for row in rows
        )
        raise RuntimeError(
            "cannot backfill approvals.reply_kind (#1459): the channel kind of these "
            f"approvals cannot be established from any binding -- {detail}. Guessing "
            "a kind would deliver the reply through the wrong adapter, which is the "
            "silent misroute this column exists to prevent, and no status is safe to "
            "guess on: approved, rejected and expired rows are exactly the ones the "
            "resume reconciler wakes, and a dead-lettered resume can re-open a row "
            "that already woke. " + report_and_guidance(rows, revision=revision)
        )

    # The provenance backfill: each row takes the kind of the binding its OWN
    # address resolves to. Every row resolves to exactly one by the check above.
    conn.execute(
        sa.text(
            f"""
            UPDATE {SCHEMA}.{TABLE} ap
            SET reply_kind = sub.kind
            FROM (
                SELECT ap2.id AS id, min(c.kind) AS kind
                FROM {SCHEMA}.{TABLE} ap2
                JOIN {SCHEMA}.{CHANNELS} c ON c.address = ap2.reply_channel
                GROUP BY ap2.id
                HAVING count(DISTINCT c.kind) = 1
            ) AS sub
            WHERE sub.id = ap.id
            """
        )
    )
    # No `SET reply_kind = 'slack' WHERE reply_kind IS NULL` sweep follows, and
    # that absence is the fix: it was the fabrication path. Any row it could still
    # reach is unreconstructable, and those aborted above; if one somehow survived,
    # the SET NOT NULL below fails loudly rather than inventing a kind for it.

    # Every backfilled row is logged for the cutover's operator-confirmation step:
    # the preflight cannot distinguish a correct match from an address that was
    # rebound since the approval was raised. Status-blind for the same reason the
    # refusal is -- a settled row's wake can still be owed.
    backfilled = conn.execute(
        sa.text(
            f"""
            SELECT id, reply_channel, reply_kind, status
            FROM {SCHEMA}.{TABLE}
            ORDER BY id
            """
        )
    ).all()
    for row in backfilled:
        logger.info(
            "approval %s (reply_channel %r) backfilled to reply_kind %r; confirm this "
            "is the kind it was RAISED on, not merely the kind its address holds now",
            row.id,
            row.reply_channel,
            row.reply_kind,
        )

    # 4. Tighten. No server_default: an insert that omits the kind must fail.
    op.alter_column(TABLE, "reply_kind", nullable=False, schema=SCHEMA)


def downgrade() -> None:
    conn = op.get_bind()

    unreconstructable = conn.execute(
        sa.text(
            f"""
            SELECT ap.id AS id,
                   ap.reply_channel AS reply_channel,
                   ap.reply_kind AS reply_kind,
                   c.kind AS bound_kind
            FROM {SCHEMA}.{TABLE} ap
            LEFT JOIN {SCHEMA}.{CHANNELS} c ON c.address = ap.reply_channel
            WHERE ap.reply_kind <> :kind
               OR (c.kind IS NOT NULL AND c.kind <> ap.reply_kind)
            ORDER BY ap.id
            """
        ),
        {"kind": SLACK},
    ).all()
    if unreconstructable:
        detail = "; ".join(
            f"{row.id} (reply_channel {row.reply_channel!r}: raised on "
            f"{row.reply_kind!r}, address now bound to "
            f"{'nothing' if row.bound_kind is None else repr(row.bound_kind)})"
            for row in unreconstructable
        )
        raise RuntimeError(
            "cannot drop approvals.reply_kind (#1459): these approvals would lose "
            f"the only record of the channel they were raised on -- {detail}. After "
            "the drop a resume re-derives the kind from the CURRENT binding, so each "
            "of these would either have no kind at all or be delivered through the "
            "wrong adapter. Resolve them, or re-point the bindings back, then "
            "re-run this downgrade."
        )

    op.drop_column(TABLE, "reply_kind", schema=SCHEMA)
    op.drop_column(TABLE, "reply_adapter", schema=SCHEMA)

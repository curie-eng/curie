"""Migration 0022 gives every durable approval its routing half.

`0022_approvals_reply_kind.py` adds `approvals.reply_adapter` (plain nullable)
and `approvals.reply_kind` (backfilled BY PROVENANCE, then set NOT NULL). It is
the durable twin of `ReplyHandle.kind`: the resume turn is rebuilt from this row,
possibly days later and possibly after the binding moved, so the kind has to be a
recorded fact about the ORIGINAL turn rather than something re-derived at resume
time (T-A8 is that half; this file is the migration half).

Three properties make it worth its own test rather than a schema diff:

1. **The backfill is by provenance, and refuses rather than guesses.** Revision 0
   of the plan justified a blanket `'slack'` on the claim that every pre-existing
   approval is Slack "by construction". That claim is false --
   `_validate_channel_binding` has accepted non-Slack kinds since phase 1, and
   phase 1's own live E2E created a `kind='email'` binding. A blind `UPDATE ...
   SET reply_kind = 'slack'` would stamp Slack onto an email approval and
   misroute its resume, which is the exact failure this whole change exists to
   kill, so an UNRECONSTRUCTABLE row aborts the migration by name.

2. **No status is provably inert, so the refusal is status-blind.** An earlier
   revision of this file asserted that a settled (approved/rejected/expired) row
   could safely take a fabricated `'slack'`, on the theory that it owes no wake.
   That theory is false in this codebase: `crud._RESUMABLE_STATUSES`
   (`crud.py:622-626`) is exactly `(approved, rejected, expired)`, so
   `resumequeue.resume_turn_for` builds a wake for all three, and
   `crud.reopen_dead_lettered_resume` clears `resumed_at` so even an
   already-resumed row can re-owe one (#532). A settled row with fabricated
   provenance therefore misroutes a LATER wake -- the same silent misroute, just
   deferred. The only backfill-to-`'slack'` path is a row whose provenance
   actually resolves to a slack binding.

3. **The downgrade destroys durable routing identity**, so it refuses in two
   directions: a non-`'slack'` kind (obviously unrepresentable) AND a `'slack'`
   row whose address is currently bound to some other kind -- after the drop, a
   resume would re-derive that row's kind from the binding and get the wrong one.

Follows `test_migration_0021_agent_channels.py`: a throwaway database all to
itself via `isolated_migration_db`, real Postgres, no mocking.
"""

from __future__ import annotations

import uuid

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_rows
from alembic import command
from alembic.config import Config

# The revision immediately below 0022, targeted explicitly rather than as a
# relative "-1": a later migration moving head would make "-1" stop short of
# undoing 0022, the backfill would never re-run on the seeded rows, and the test
# would go green while proving nothing (the trap #1391 documents).
BELOW = "0021"
REVISION = "0022"

# The unique constraint on `agent_channels.address` that 0021 created and 0023
# replaces. 0022 runs while it is still in force, so the "one address, two
# kinds" ambiguity it must detect is only reachable by dropping it out of band --
# which is exactly the pre-0017-style restored dump the pre-flight exists for.
ADDRESS_CONSTRAINT = "agent_channels_address_key"


def _seed_binding(name: str, kind: str, address: str) -> uuid.UUID:
    """One agent and its `agent_channels` row, on the pre-0022 schema."""

    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": name},
    )
    sql_rows(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
        "VALUES (:id, :agent, :kind, :addr)",
        {"id": uuid.uuid4(), "agent": agent_id, "kind": kind, "addr": address},
    )
    return agent_id


def _seed_approval(*, reply_channel: str, status: str, summary: str) -> uuid.UUID:
    """One approval on the PRE-0022 schema (no `reply_kind` column yet)."""

    approval_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.approvals "
        "(id, conversation_id, author, summary, reply_channel, reply_placeholder, "
        " dedupe_key, status) "
        "VALUES (:id, :conv, :author, :summary, :chan, :ph, :dedupe, :status)",
        {
            "id": approval_id,
            "conv": f"th-{approval_id.hex[:8]}",
            "author": "U1",
            "summary": summary,
            "chan": reply_channel,
            "ph": "p-1",
            "dedupe": approval_id.hex,
            "status": status,
        },
    )
    return approval_id


def _reply_kind(approval_id: uuid.UUID) -> str | None:
    rows = sql_rows(
        "SELECT reply_kind FROM curie.approvals WHERE id = :id", {"id": approval_id}
    )
    assert rows, f"approval {approval_id} vanished"
    return rows[0][0]


def _is_nullable(table: str, column: str) -> bool:
    rows = sql_rows(
        "SELECT is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :t AND column_name = :c",
        {"t": table, "c": column},
    )
    assert rows, f"curie.{table}.{column} does not exist"
    return rows[0][0] == "YES"


def _at_below(db: IsolatedMigrationDb) -> Config:
    """A config with the database sitting exactly at the revision below 0022."""

    db.at(BELOW)
    return alembic_config()


def test_the_upgrade_backfills_each_approval_from_its_own_binding(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A7 / AC3, the provenance half.

    Two approvals, two bindings, two different kinds. Each row takes the kind of
    the binding ITS OWN address resolves to -- which a blind `SET reply_kind =
    'slack'` gets right for exactly one of them and silently wrong for the other.
    """

    cfg = _at_below(isolated_migration_db)
    _seed_binding("mail-agent", "email", "ops@example.test")
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    email_approval = _seed_approval(
        reply_channel="ops@example.test", status="pending", summary="send the quote"
    )
    slack_approval = _seed_approval(
        reply_channel="C0EXAMPLE1", status="pending", summary="give a discount"
    )

    command.upgrade(cfg, REVISION)

    assert _reply_kind(email_approval) == "email"
    assert _reply_kind(slack_approval) == "slack"

    # The column is NOT NULL after the backfill (no server_default, deliberately:
    # an old API pod inserting during the window must fail loudly rather than
    # fabricate a kind -- plan edge case E4).
    assert not _is_nullable("approvals", "reply_kind")

    # `reply_adapter` is added in the same revision, nullable, with NO backfill:
    # Slack legitimately has no adapter, and nothing in the schema records what a
    # pre-0022 approval's adapter would have been.
    assert _is_nullable("approvals", "reply_adapter")
    adapters = sql_rows("SELECT DISTINCT reply_adapter FROM curie.approvals")
    assert [row[0] for row in adapters] == [None]


@pytest.mark.parametrize("status", ["approved", "rejected", "expired"])
def test_the_upgrade_refuses_an_unreconstructable_row_of_any_settled_status(
    isolated_migration_db: IsolatedMigrationDb, status: str
) -> None:
    """T-A10, status-blind half / AC3.

    A settled row is NOT inert here, which is the whole correction. All three of
    these statuses are in `crud._RESUMABLE_STATUSES` (`crud.py:622-626`), so
    `resumequeue.resume_turn_for` builds a wake for each, and
    `crud.reopen_dead_lettered_resume` can clear `resumed_at` on a row whose
    resume was dead-lettered (#532), putting it back on the reconciler's
    work-list. Fabricating `'slack'` on a row nobody can vouch for therefore
    buys a misroute later instead of an abort now.

    All three statuses are parametrized rather than tested on one representative
    because the plausible wrong implementation is a status allow-list, and an
    allow-list gets one status right by accident.
    """

    cfg = _at_below(isolated_migration_db)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    healthy = _seed_approval(
        reply_channel="C0EXAMPLE1", status=status, summary="resolvable one"
    )
    settled = _seed_approval(
        reply_channel="nobody@example.test", status=status, summary="long since decided"
    )

    with pytest.raises(Exception) as caught:
        command.upgrade(cfg, REVISION)

    message = str(caught.value)
    assert str(settled) in message, message
    assert "nobody@example.test" in message, message
    assert str(healthy) not in message, message


@pytest.mark.parametrize("status", ["pending", "approved", "rejected", "expired"])
def test_the_only_backfill_to_slack_is_a_row_bound_to_a_slack_binding(
    isolated_migration_db: IsolatedMigrationDb, status: str
) -> None:
    """T-A7's other half, and the positive control for the refusal above.

    `'slack'` is written when, and only when, the row's own address resolves to a
    slack binding -- earned provenance, never a default. Without this the
    migration could satisfy every refusal test by aborting on everything, and
    without the status sweep a status-gated backfill would go unnoticed on the
    lane it skipped.
    """

    cfg = _at_below(isolated_migration_db)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    approval = _seed_approval(
        reply_channel="C0EXAMPLE1", status=status, summary="ordinary slack"
    )

    command.upgrade(cfg, REVISION)

    assert _reply_kind(approval) == "slack"


def test_the_upgrade_refuses_a_pending_approval_that_matches_no_binding(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A10, refusal half / AC3. A pending row carries a live resume
    obligation, and guessing its kind is the silent misroute. (The refusal is
    status-blind -- see the settled-status sweep below -- but pending is the
    shape an operator actually meets during the cutover.)

    Mutation this catches: replace the pre-flight with a blind `UPDATE` and the
    migration succeeds, stamping `'slack'` on a row nobody can vouch for.

    The error must NAME the row -- the operator's next step is
    `POST /approvals/{id}/resolve` on exactly these ids (cutover step 7), and
    there is deliberately no force-through flag.
    """

    cfg = _at_below(isolated_migration_db)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    healthy = _seed_approval(
        reply_channel="C0EXAMPLE1", status="pending", summary="fine one"
    )
    orphan = _seed_approval(
        reply_channel="nobody@example.test", status="pending", summary="orphaned one"
    )

    with pytest.raises(Exception) as caught:
        command.upgrade(cfg, REVISION)

    message = str(caught.value)
    assert str(orphan) in message, message
    assert "nobody@example.test" in message, message
    # The healthy row is not blamed for its neighbour's state.
    assert str(healthy) not in message, message


def test_the_upgrade_refuses_a_pending_approval_whose_address_spans_two_kinds(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A10, the ambiguous-provenance half.

    One address, two bindings, two kinds: the join resolves to more than one
    kind, so there is no honest value to write. Reachable only out of band while
    `agent_channels_address_key` stands (0023 is what makes it ordinary), and out
    of band is exactly the state a migration has to survive -- a database
    restored from a dump taken after 0023 and rolled back is precisely this
    shape.
    """

    cfg = _at_below(isolated_migration_db)
    _seed_binding("slack-agent", "slack", "shared@example.test")
    sql_rows(f"ALTER TABLE curie.agent_channels DROP CONSTRAINT {ADDRESS_CONSTRAINT}")
    _seed_binding("mail-agent", "email", "shared@example.test")
    ambiguous = _seed_approval(
        reply_channel="shared@example.test", status="pending", summary="which one?"
    )

    with pytest.raises(Exception) as caught:
        command.upgrade(cfg, REVISION)

    message = str(caught.value)
    assert str(ambiguous) in message, message
    assert "shared@example.test" in message, message


def test_the_downgrade_refuses_while_any_approval_is_not_slack(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A11 / finding 17. Dropping the column destroys durable routing identity
    for exactly the rows that need it: after the drop, an email approval's resume
    has no way to know it is an email approval.
    """

    cfg = _at_below(isolated_migration_db)
    _seed_binding("mail-agent", "email", "ops@example.test")
    email_approval = _seed_approval(
        reply_channel="ops@example.test", status="pending", summary="send the quote"
    )
    command.upgrade(cfg, REVISION)
    assert _reply_kind(email_approval) == "email"

    with pytest.raises(Exception) as caught:
        command.downgrade(cfg, BELOW)

    message = str(caught.value)
    assert str(email_approval) in message, message
    assert "email" in message, message
    # And the refusal was total: the column still holds its values.
    assert _reply_kind(email_approval) == "email"


def test_the_downgrade_refuses_a_slack_row_whose_address_now_binds_another_kind(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A11, the round-2 predicate. This row is `'slack'`, so the first
    predicate passes it -- and it is still unreconstructable: its address is
    currently bound to `email`, so a post-drop resume would re-derive `email` and
    deliver a Slack turn through the mail adapter. Same silent misroute, opposite
    direction.
    """

    cfg = _at_below(isolated_migration_db)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    approval = _seed_approval(
        reply_channel="C0EXAMPLE1", status="pending", summary="raised on slack"
    )
    command.upgrade(cfg, REVISION)
    assert _reply_kind(approval) == "slack"

    # The operator re-points that address at a different adapter (`crud.
    # update_agent_binding` mutates the row in place, so this is an ordinary
    # PATCH, not an exotic state).
    sql_rows(
        "UPDATE curie.agent_channels SET kind = 'email' WHERE address = 'C0EXAMPLE1'"
    )

    with pytest.raises(Exception) as caught:
        command.downgrade(cfg, BELOW)

    message = str(caught.value)
    assert str(approval) in message, message
    assert "C0EXAMPLE1" in message, message


def test_the_downgrade_round_trips_when_every_row_is_reconstructable(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A11's positive control. Without this, a `downgrade` that raised
    unconditionally would pass every refusal test above while being unusable.
    """

    cfg = _at_below(isolated_migration_db)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    approval = _seed_approval(
        reply_channel="C0EXAMPLE1", status="pending", summary="ordinary slack"
    )

    command.upgrade(cfg, REVISION)
    command.downgrade(cfg, BELOW)

    rows = sql_rows(
        "SELECT 1 FROM information_schema.columns WHERE table_schema = 'curie' "
        "AND table_name = 'approvals' AND column_name IN ('reply_kind', 'reply_adapter')"
    )
    assert rows == []

    # Re-upgrading finds the row still there and backfills it identically.
    command.upgrade(cfg, REVISION)
    assert _reply_kind(approval) == "slack"

"""The identity-bearing migrations fence `approvals` and `agent_channels`.

`curie_api.migration_fence` is a helper imported BY revisions 0022 and 0024
(importable because `alembic.ini` sets `prepend_sys_path = src`). It does
two things this file proves through the real migrations, never through a bare
`LOCK` statement:

1. **`fence_identity_tables(conn)`** takes `SET LOCAL lock_timeout` then `LOCK
   TABLE curie.approvals, curie.agent_channels IN ACCESS EXCLUSIVE MODE` at
   the top of `upgrade()`. The preflight, the backfill and the constraint
   tightening then commit as one unit behind it, so a binding cannot move
   between the preflight's answer and the backfill that records it.

   The property that matters operationally is the one the deleted 503 design
   destroyed: a concurrent writer **BLOCKS and then SUCCEEDS**. Routine
   maintenance never turns an in-flight turn's approval request into a
   refusal. A bounded `lock_timeout` is the other half -- if the fence cannot
   be taken, the migration refuses BEFORE mutating anything and names what held
   the lock, rather than wedging a Job that has an `activeDeadlineSeconds`.

   **ACCESS EXCLUSIVE, not SHARE ROW EXCLUSIVE, and the difference is an abort.**
   A weaker fence followed by the revision's own `ADD COLUMN` is a lock UPGRADE,
   and an ordinary resolver that READS an approval and then WRITES it closes the
   cycle: the fence is granted past the reader's ACCESS SHARE, the ADD COLUMN
   then waits on that reader, and the reader's UPDATE waits on the fence.
   PostgreSQL aborts one participant -- routine maintenance killing a live
   resolution, which is exactly what AC1 forbids.
   `test_a_reader_that_becomes_a_writer_is_not_deadlocked_by_the_fence` is that
   shape, and it fails on the old mode. The cost is that a concurrent plain
   SELECT now blocks for the fence's duration too; that is asserted here as the
   real behavior rather than wished away.

2. **Provenance declarations.** A human-authored, per-row document at
   `CURIE_APPROVAL_PROVENANCE_DECLARATIONS` states the `reply_kind` /
   `reply_adapter` an approval was RAISED on. The migration honors one for
   EXACTLY the rows its preflight could not reconstruct and refuses everything
   else -- a declaration for a reconstructable row (the preflight's own answer
   wins), a declaration for an id it does not name at all (a stale file), and a
   malformed one. Every honored declaration appends one
   `approval_audit_entries` row, so the bypass is recorded rather than silent,
   and nothing is ever deleted.

Two traps this file is written around, because a test that misses either is
worthless:

- **The negative control.** 0022 runs `ALTER TABLE curie.approvals ADD COLUMN`,
  which takes ACCESS EXCLUSIVE on `approvals` all by itself. A concurrent
  INSERT into `approvals` therefore blocks whether or not the fence exists, so
  that test alone cannot prove the fence. `curie.agent_channels` is the control:
  0022 never touches it, so a concurrent `agent_channels` write blocks ONLY
  because `fence_identity_tables` locked it. Same for the rival-lock refusal:
  without the fence, 0022 completes happily with a lock held on
  `agent_channels`.

- **Pausing the migration mid-fence.** Following
  `test_migration_0034_approval_route_targets.py:133-200` (hold the migration
  with `pg_sleep`, synchronize on `pg_stat_activity`), but hooked on
  `curie.alembic_version` rather than on a table the revision happens to write.
  Alembic stamps the version inside the SAME transaction
  (`env.py:61` `transaction_per_migration=True`), so a BEFORE UPDATE trigger
  there fires while the fence is still held, and it works identically for a
  revision that backfills (0022) and one that does not (0024).

Follows `test_migration_0022_approvals_reply_kind.py`: a throwaway database all
to itself via `isolated_migration_db`, real Postgres, no mocking. Every wait is
bounded so a hang FAILS rather than wedging CI.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from curie_api.config import get_settings
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import text

ALEMBIC_DIR = Path(__file__).resolve().parents[1] / "alembic"

# Targeted explicitly, never as a relative "-1" (#1391).
BELOW_0022 = "0021a"
REVISION_0022 = "0022"
BELOW_0024 = "0023"
REVISION_0024 = "0024"

ADDRESS_CONSTRAINT = "agent_channels_address_key"

# The env names the plan fixes. They are asserted here, not merely used, so a
# rename lands as a failing test rather than as a silently inert override.
LOCK_TIMEOUT_ENV = "CURIE_MIGRATION_FENCE_LOCK_TIMEOUT_MS"
DECLARATIONS_ENV = "CURIE_APPROVAL_PROVENANCE_DECLARATIONS"

# Columns `approval_audit_entries` has at 0013 (`evidence` arrives there). The
# fence helper must write NOTHING outside this set from 0022/0024:
# `principal_kind` and `authenticated` are created by 0038, which runs LATER.
AUDIT_COLUMNS_AT_0013 = {
    "id",
    "approval_id",
    "action",
    "actor",
    "actor_channel",
    "decision",
    "authorizer",
    "authorized",
    "reason",
    "evidence",
    "created_at",
}

HONORED_ACTION = "provenance_declaration_honored"
FENCE_AUTHORIZER = "migration_provenance_fence"

# How long a paused migration holds the fence. Long enough to observe a waiter
# in `pg_stat_activity`, short enough that the suite stays quick.
PAUSE_SECONDS = 2.0
# Every poll and every join in this file is bounded by this, so a fence that
# never releases fails the test instead of hanging the run.
BOUND_SECONDS = 20.0


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[Any]:
    """Run one statement against the isolated migration database."""

    async def _go() -> list[Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params or {})
                return list(result.all()) if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(_go())


def _alembic_config() -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(ALEMBIC_DIR))
    return cfg


def _at(revision: str) -> Config:
    cfg = _alembic_config()
    command.upgrade(cfg, revision)
    return cfg


def _seed_binding(name: str, kind: str, address: str) -> uuid.UUID:
    agent_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": name},
    )
    _sql(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
        "VALUES (:id, :agent, :kind, :addr)",
        {"id": uuid.uuid4(), "agent": agent_id, "kind": kind, "addr": address},
    )
    return agent_id


def _seed_approval(
    *,
    reply_channel: str,
    status: str = "pending",
    summary: str = "decide this",
    reply_kind: str | None = None,
) -> uuid.UUID:
    """One approval. `reply_kind` is named only at/after 0022, where it is NOT
    NULL; the column does not exist below that, so the statement cannot mention
    it there."""

    approval_id = uuid.uuid4()
    columns = "" if reply_kind is None else ", reply_kind"
    values = "" if reply_kind is None else ", :kind"
    _sql(
        "INSERT INTO curie.approvals "
        f"(id, conversation_id, author, summary, reply_channel, reply_placeholder, "
        f" dedupe_key, status{columns}) "
        f"VALUES (:id, :conv, :author, :summary, :chan, :ph, :dedupe, :status{values})",
        {
            **({} if reply_kind is None else {"kind": reply_kind}),
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


def _seed_audit_row(approval_id: uuid.UUID, action: str) -> uuid.UUID:
    entry_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.approval_audit_entries "
        "(id, approval_id, action, actor, decision, authorizer, authorized, reason) "
        "VALUES (:id, :ap, :action, 'U0HUMAN', 'none', 'route', true, 'pre-existing')",
        {"id": entry_id, "ap": approval_id, "action": action},
    )
    return entry_id


def _audit_rows(approval_id: uuid.UUID) -> list[Any]:
    return _sql(
        "SELECT id, action, actor, decision, authorizer, authorized, reason, evidence "
        "FROM curie.approval_audit_entries WHERE approval_id = :id ORDER BY created_at, id",
        {"id": approval_id},
    )


def _audit_table_columns() -> set[str]:
    rows = _sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'approval_audit_entries'"
    )
    return {row[0] for row in rows}


def _approvals_columns() -> set[str]:
    rows = _sql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'approvals'"
    )
    return {row[0] for row in rows}


def _stamped_revision() -> str | None:
    rows = _sql("SELECT version_num FROM curie.alembic_version")
    return rows[0][0] if rows else None


def _reply_identity(approval_id: uuid.UUID) -> tuple[str | None, str | None]:
    rows = _sql(
        "SELECT reply_kind, reply_adapter FROM curie.approvals WHERE id = :id",
        {"id": approval_id},
    )
    assert rows, f"approval {approval_id} vanished"
    return rows[0][0], rows[0][1]


# --------------------------------------------------------------------------
# Declaration documents.
#
# The shape is stated here once because the migration, the CLI reporter
# (`curie cluster approvals --report-identity`) and the operator all have to
# agree on it. Every field is required: the point of the document is that a
# human vouched for the row, so an anonymous or unexplained declaration is not
# a weaker declaration, it is not one at all.
# --------------------------------------------------------------------------


def _declaration(
    approval_id: uuid.UUID,
    *,
    reply_kind: str,
    reply_adapter: str | None,
    actor: str = "U0OPERATOR",
    reason: str = "raised on the mail bridge before the binding was re-pointed",
) -> dict[str, Any]:
    return {
        "approval_id": str(approval_id),
        "reply_kind": reply_kind,
        "reply_adapter": reply_adapter,
        "actor": actor,
        "reason": reason,
    }


def _write_declarations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    declarations: list[dict[str, Any]] | str,
) -> Path:
    path = tmp_path / "provenance-declarations.json"
    if isinstance(declarations, str):
        path.write_text(declarations)
    else:
        path.write_text(json.dumps({"declarations": declarations}))
    monkeypatch.setenv(DECLARATIONS_ENV, str(path))
    return path


# --------------------------------------------------------------------------
# Mid-fence pause + bounded concurrency probes.
# --------------------------------------------------------------------------


def _install_version_stamp_pause(revision: str) -> None:
    """Hold the migration inside its own transaction, fence still taken.

    Alembic stamps `curie.alembic_version` in the same transaction as the
    revision body (`env.py:61`), so sleeping in a BEFORE UPDATE trigger there
    pauses the migration with every lock it took still held -- including the
    fence -- and works for a revision that backfills and one that does not.
    """

    _sql(
        f"""
        CREATE FUNCTION curie.pause_at_version_stamp() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.version_num = '{revision}' THEN
                PERFORM pg_sleep({PAUSE_SECONDS});
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    _sql(
        """
        CREATE TRIGGER pause_at_version_stamp
        BEFORE UPDATE ON curie.alembic_version
        FOR EACH ROW EXECUTE FUNCTION curie.pause_at_version_stamp()
        """
    )


def _await_paused_migration(migration: Any) -> None:
    """Block until the migration is inside the pause, or fail. Never hangs."""

    deadline = time.monotonic() + BOUND_SECONDS
    while not _sql(
        "SELECT 1 FROM pg_stat_activity WHERE wait_event = 'PgSleep' "
        "AND datname = current_database()"
    ):
        if migration.done():
            migration.result()
            pytest.fail("the migration finished before it could be observed mid-fence")
        assert time.monotonic() < deadline, "the migration never reached the pause"
        time.sleep(0.02)


def _await_waiting_on_lock(probe: Any, needle: str) -> None:
    """Assert a concurrent statement is parked on a LOCK, not merely slow."""

    deadline = time.monotonic() + BOUND_SECONDS
    while True:
        waiting = _sql(
            "SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
            "AND datname = current_database() AND query LIKE :needle",
            {"needle": f"%{needle}%"},
        )
        if waiting:
            return
        if probe.done():
            probe.result()
            pytest.fail(
                f"the concurrent {needle!r} was never blocked by the fence -- it "
                "completed while the migration held the identity tables"
            )
        assert time.monotonic() < deadline, f"{needle!r} never parked on a lock"
        time.sleep(0.02)


def _run_statement(statement: str, params: dict[str, Any] | None = None) -> None:
    """A second connection's write, with a lock_timeout that outlasts the pause.

    Generous on purpose: the assertion is that the writer WAITS and then wins.
    If the fence ever refused a writer instead of queuing it, this raises and
    the test fails, which is the whole point.
    """

    async def _go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(f"SET LOCAL lock_timeout = '{int(BOUND_SECONDS * 1000)}ms'")
                )
                await conn.execute(text(statement), params or {})
        finally:
            await engine.dispose()

    asyncio.run(_go())


def _hold_conflicting_lock(table: str, ready: Any, release: Any) -> None:
    """Park an EXCLUSIVE lock on `table` until `release` is set."""

    async def _go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text(f"LOCK TABLE {table} IN EXCLUSIVE MODE"))
                ready.set()
                while not release.is_set():
                    await asyncio.sleep(0.02)
        finally:
            await engine.dispose()

    asyncio.run(_go())


# ==========================================================================
# The fence: blocks, then admits. AC2.
# ==========================================================================


def test_a_concurrent_approval_insert_blocks_on_the_fence_and_then_succeeds(
    isolated_migration_db: None,
) -> None:
    """AC1/AC2's load-bearing property, and the replacement for every 503.

    An in-flight turn's `POST /approvals` reaches `crud.create_approval` while
    the migration runs. It must WAIT and then be accepted. Refusing it converts
    a pending decision into a terminal escalation (`HttpApprovalClient.create`
    maps every non-2xx to `ApprovalBackendError`), which is exactly the harm
    AC1 forbids for routine maintenance.

    Driven against 0024, and that is deliberate on two counts. 0024 ALTERs only
    `agent_channels`, so the block here is caused by `fence_identity_tables`
    and by nothing else -- against 0022 this same probe would block on the
    `ALTER TABLE curie.approvals ADD COLUMN`'s own ACCESS EXCLUSIVE lock and
    prove nothing. And `approvals.reply_kind` is already NOT NULL at 0023, so
    the raced INSERT is shaped exactly like the live one it stands in for.
    """

    cfg = _at(BELOW_0024)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    _install_version_stamp_pause(REVISION_0024)

    raced = uuid.uuid4()
    insert = (
        "INSERT INTO curie.approvals "
        "(id, conversation_id, author, summary, reply_channel, reply_kind, "
        " reply_placeholder, dedupe_key, status) "
        "VALUES (:id, 'th-raced', 'U2', 'raced in', 'C0EXAMPLE1', 'slack', 'p-1', "
        " :dedupe, 'pending')"
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        migration = pool.submit(command.upgrade, cfg, REVISION_0024)
        _await_paused_migration(migration)
        probe = pool.submit(_run_statement, insert, {"id": raced, "dedupe": raced.hex})
        _await_waiting_on_lock(probe, "INSERT INTO curie.approvals")
        migration.result(timeout=BOUND_SECONDS)
        probe.result(timeout=BOUND_SECONDS)

    # It was queued, never refused.
    assert _sql("SELECT status FROM curie.approvals WHERE id = :id", {"id": raced}) == [
        ("pending",)
    ]
    # And it landed AFTER the preflight, so it was never in the preflight's set.
    assert _stamped_revision() == REVISION_0024


def _read_then_write(read_done: Any, proceed: Any) -> None:
    """One transaction that SELECTs an approval, pauses, then UPDATEs it.

    The ordinary resolver's shape: `crud` reads the approval row inside its
    transaction before it writes the decision. The read takes ACCESS SHARE and
    holds it to commit; the write then wants ROW EXCLUSIVE on the same table.
    """

    async def _go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(f"SET LOCAL lock_timeout = '{int(BOUND_SECONDS * 1000)}ms'")
                )
                await conn.execute(text("SELECT count(*) FROM curie.approvals"))
                read_done.set()
                while not proceed.is_set():
                    await asyncio.sleep(0.02)
                await conn.execute(
                    text("UPDATE curie.approvals SET reply_placeholder = 'resolved'")
                )
        finally:
            await engine.dispose()

    asyncio.run(_go())


def test_a_reader_that_becomes_a_writer_is_not_deadlocked_by_the_fence(
    isolated_migration_db: None,
) -> None:
    """The deadlock the weaker fence created, as a test. AC1/AC2.

    A resolver holds ACCESS SHARE on `curie.approvals` from its read. Under
    SHARE ROW EXCLUSIVE the fence is GRANTED past it (the modes do not
    conflict), 0022's own `ALTER TABLE curie.approvals ADD COLUMN` then waits on
    that reader, and the reader's UPDATE waits on the fence. That is a hard
    cycle and PostgreSQL resolves it by ABORTING one side -- either the
    migration or a live approval resolution, neither of which routine
    maintenance is allowed to do.

    Taking the strongest mode the revision needs UP FRONT removes the later
    upgrade, so the only wait is the fence's own, which the deadlock detector
    resolves by ordering rather than by killing anybody. Both sides complete.

    Run this against the SHARE ROW EXCLUSIVE fence and it fails with
    `DeadlockDetected` from whichever participant PostgreSQL picked.
    """

    import threading

    cfg = _at(BELOW_0022)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    approval = _seed_approval(reply_channel="C0EXAMPLE1")

    read_done = threading.Event()
    proceed = threading.Event()

    with ThreadPoolExecutor(max_workers=2) as pool:
        resolver = pool.submit(_read_then_write, read_done, proceed)
        assert read_done.wait(timeout=BOUND_SECONDS), "the resolver never read"

        migration = pool.submit(command.upgrade, cfg, REVISION_0022)
        # Wait until the migration is actually parked on a lock -- under the old
        # mode that is the ADD COLUMN, under the new one it is the fence itself.
        # Either way it is the state from which releasing the resolver's write
        # closes the cycle.
        deadline = time.monotonic() + BOUND_SECONDS
        while not _sql(
            "SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
            "AND datname = current_database() AND query LIKE '%curie.approvals%'"
        ):
            if migration.done():
                migration.result()
                pytest.fail("the migration finished before the resolver could contend")
            assert time.monotonic() < deadline, "the migration never contended"
            time.sleep(0.02)

        proceed.set()
        # Neither participant is aborted. A deadlock surfaces as an exception
        # from exactly one of these two `result()` calls.
        resolver.result(timeout=BOUND_SECONDS)
        migration.result(timeout=BOUND_SECONDS)

    assert _stamped_revision() == REVISION_0022
    # The resolver's write survived, and the approval kept the identity the
    # migration established behind the fence.
    assert _sql(
        "SELECT reply_placeholder FROM curie.approvals WHERE id = :id", {"id": approval}
    ) == [("resolved",)]
    assert _reply_identity(approval) == ("slack", None)


def test_the_fence_refuses_within_its_timeout_and_mutates_nothing(
    isolated_migration_db: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC2's bound, and the second negative control.

    A rival holds `curie.agent_channels`. 0022 does not otherwise touch that
    table, so without the fence this migration sails through and the test
    fails. With it, the migration refuses BEFORE mutating -- naming the blocker
    read from `pg_stat_activity`, not a bare "lock timeout" -- and the schema
    and rows are exactly as they were.
    """

    monkeypatch.setenv(LOCK_TIMEOUT_ENV, "500")
    cfg = _at(BELOW_0022)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    approval = _seed_approval(reply_channel="C0EXAMPLE1")

    columns_before = _approvals_columns()
    rows_before = _sql("SELECT id, status, reply_channel FROM curie.approvals ORDER BY id")

    import threading

    ready = threading.Event()
    release = threading.Event()
    with ThreadPoolExecutor(max_workers=2) as pool:
        holder = pool.submit(_hold_conflicting_lock, "curie.agent_channels", ready, release)
        assert ready.wait(timeout=BOUND_SECONDS), "the rival never took its lock"
        try:
            started = time.monotonic()
            with pytest.raises(RuntimeError) as caught:
                command.upgrade(cfg, REVISION_0022)
            elapsed = time.monotonic() - started
        finally:
            release.set()
        holder.result(timeout=BOUND_SECONDS)

    message = str(caught.value)
    assert "agent_channels" in message, message
    assert "LOCK TABLE" in message or "EXCLUSIVE" in message, message
    # Bounded by the override, not by the 15 s default and not unbounded.
    assert elapsed < BOUND_SECONDS / 2, elapsed

    # Nothing moved: no new columns, no stamped revision, no touched rows.
    assert _approvals_columns() == columns_before
    assert _stamped_revision() == BELOW_0022
    assert _sql("SELECT id, status, reply_channel FROM curie.approvals ORDER BY id") == rows_before
    assert _sql("SELECT id FROM curie.approvals WHERE id = :id", {"id": approval})


# ==========================================================================
# Provenance declarations. AC4.
# ==========================================================================


def _seed_unreconstructable() -> uuid.UUID:
    """A row whose address resolves to TWO kinds, so no honest value exists.

    Deliberately two kinds rather than none: it gives the declaration something
    to beat. If the migration ever fell back to inference it would write
    'email' or 'slack' here, and the declared kind below is neither.
    """

    _seed_binding("slack-agent", "slack", "shared@example.test")
    _sql(f"ALTER TABLE curie.agent_channels DROP CONSTRAINT {ADDRESS_CONSTRAINT}")
    _seed_binding("mail-agent", "email", "shared@example.test")
    return _seed_approval(reply_channel="shared@example.test", summary="which one?")


def test_without_a_declaration_the_refusal_names_the_report_and_declare_workflow(
    isolated_migration_db: None,
) -> None:
    """AC4, "never fabricate provenance", plus the disposition the operator gets.

    Unchanged behavior: an unreconstructable row still stops the migration. What
    changes is the sentence. "delete the approvals" was not a disposition -- it
    destroys the audit history AC1 requires preserved, and on a blocked
    installation the recovery API is not even installed yet. The refusal now
    names the bounded round trip instead.
    """

    cfg = _at(BELOW_0022)
    ambiguous = _seed_unreconstructable()

    with pytest.raises(RuntimeError) as caught:
        command.upgrade(cfg, REVISION_0022)

    message = str(caught.value)
    assert str(ambiguous) in message, message
    assert "--report-identity" in message, message
    assert "delete the approvals" not in message, message
    # The row is still there to be declared over.
    assert _sql("SELECT count(*) FROM curie.approvals") == [(1,)]


def test_a_declaration_for_an_unreconstructable_row_beats_what_a_binding_implies(
    isolated_migration_db: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC4's positive case. The declared kind is neither kind at the address.

    'sms' matches no binding in this database, so it cannot have been inferred:
    a helper that quietly preferred the binding would write 'slack' or 'email'
    and this fails. That is the difference between honoring a human statement
    and guessing.
    """

    cfg = _at(BELOW_0022)
    ambiguous = _seed_unreconstructable()
    _write_declarations(
        tmp_path,
        monkeypatch,
        [
            _declaration(
                ambiguous,
                reply_kind="sms",
                reply_adapter="twilio-main",
                actor="U0OPERATOR",
                reason="raised on the SMS bridge; the address was reused later",
            )
        ],
    )

    command.upgrade(cfg, REVISION_0022)

    assert _reply_identity(ambiguous) == ("sms", "twilio-main")
    assert _stamped_revision() == REVISION_0022


def test_a_honored_declaration_writes_exactly_one_audit_row_and_deletes_nothing(
    isolated_migration_db: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC4, "never silently bypass a migration guard" and "never delete history".

    One append-only row, carrying the declared identity, the declaring actor,
    the reason, and a non-null authorizer -- the bypass is attributed and
    reviewable. The pre-existing audit trail and the approval itself survive
    untouched, which is also AC1's audit-history clause.
    """

    cfg = _at(BELOW_0022)
    ambiguous = _seed_unreconstructable()
    pre_existing = _seed_audit_row(ambiguous, "created")
    _write_declarations(
        tmp_path,
        monkeypatch,
        [
            _declaration(
                ambiguous,
                reply_kind="sms",
                reply_adapter="twilio-main",
                actor="U0OPERATOR",
                reason="raised on the SMS bridge; the address was reused later",
            )
        ],
    )

    command.upgrade(cfg, REVISION_0022)

    rows = _audit_rows(ambiguous)
    assert [row.id for row in rows][0] == pre_existing, "history was rewritten"
    honored = [row for row in rows if row.action == HONORED_ACTION]
    assert len(honored) == 1, rows
    entry = honored[0]
    assert entry.actor == "U0OPERATOR"
    assert entry.authorizer == FENCE_AUTHORIZER
    assert entry.authorized is True
    assert entry.reason is not None and entry.reason.strip()
    assert entry.evidence["declared_reply_kind"] == "sms"
    assert entry.evidence["declared_reply_adapter"] == "twilio-main"
    assert entry.evidence["revision"] == REVISION_0022
    # Why the preflight could not reconstruct it -- the operator's own record of
    # what they were vouching over.
    assert entry.evidence["preflight_reason"]

    # Nothing deleted, and no column written that does not exist at 0022:
    # `principal_kind` / `authenticated` arrive at 0038, three revisions later.
    assert _sql("SELECT count(*) FROM curie.approvals") == [(1,)]
    assert _audit_table_columns() == AUDIT_COLUMNS_AT_0013


def test_a_declaration_for_a_reconstructable_row_is_refused_by_name(
    isolated_migration_db: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC4's honoring rule: the preflight's own answer wins, loudly.

    A human overriding provenance the schema can still prove is not a recovery,
    it is a rewrite. The migration stops rather than accept it, and the
    reconstructable row keeps nothing from the declaration.
    """

    cfg = _at(BELOW_0022)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    reconstructable = _seed_approval(reply_channel="C0EXAMPLE1")
    _write_declarations(
        tmp_path,
        monkeypatch,
        [_declaration(reconstructable, reply_kind="sms", reply_adapter="twilio-main")],
    )

    with pytest.raises(RuntimeError) as caught:
        command.upgrade(cfg, REVISION_0022)

    message = str(caught.value)
    assert str(reconstructable) in message, message
    assert _stamped_revision() == BELOW_0022
    assert "reply_kind" not in _approvals_columns()


def test_0024_honors_a_declaration_for_its_own_unroutable_rows(
    isolated_migration_db: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """0024's preflight is a different question, and takes the same disposition.

    Its unreconstructable set is non-Slack approvals with no adapter-bearing
    binding, and its refusal used to say "DRAIN them" -- settle every pending
    approval -- which is the forced resolution AC1 forbids for routine
    maintenance. The declaration replaces it, and its audit row is written with
    the 0013 column set too.
    """

    cfg = _at(BELOW_0022)
    _seed_binding("mail-agent", "email", "ops@example.test")
    email_approval = _seed_approval(reply_channel="ops@example.test")
    command.upgrade(cfg, BELOW_0024)

    _write_declarations(
        tmp_path,
        monkeypatch,
        [
            _declaration(
                email_approval,
                reply_kind="email",
                reply_adapter="smtp-primary",
                reason="raised on the smtp-primary egress, retired since",
            )
        ],
    )

    command.upgrade(cfg, REVISION_0024)

    assert _reply_identity(email_approval) == ("email", "smtp-primary")
    honored = [r for r in _audit_rows(email_approval) if r.action == HONORED_ACTION]
    assert len(honored) == 1
    assert honored[0].evidence["revision"] == REVISION_0024
    assert _audit_table_columns() == AUDIT_COLUMNS_AT_0013


def test_0024_refuses_a_declaration_that_contradicts_the_kind_0022_established(
    isolated_migration_db: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC1/AC4: 0024 recovers the MISSING half, it does not rewrite the known one.

    This approval's address resolves to an `email` binding, so 0022 established
    `reply_kind = 'email'` from its own provenance. 0024's question is narrower
    -- no binding names the egress identity its reply must be authenticated
    with -- and the only thing a declaration may supply there is the adapter.
    Accepting a declared `slack` would replace a recoverable original identity
    with an operator's later guess, which is the rewrite AC4 forbids, and would
    silently reroute the reply to a different transport entirely.
    """

    cfg = _at(BELOW_0022)
    _seed_binding("mail-agent", "email", "ops@example.test")
    email_approval = _seed_approval(reply_channel="ops@example.test")
    command.upgrade(cfg, BELOW_0024)
    assert _reply_identity(email_approval) == ("email", None)

    _write_declarations(
        tmp_path,
        monkeypatch,
        [_declaration(email_approval, reply_kind="slack", reply_adapter=None)],
    )

    with pytest.raises(RuntimeError) as caught:
        command.upgrade(cfg, REVISION_0024)

    message = str(caught.value)
    assert str(email_approval) in message, message
    assert "'email'" in message and "'slack'" in message, message
    # Nothing moved: the established kind is intact and 0024 did not stamp.
    assert _reply_identity(email_approval) == ("email", None)
    assert _stamped_revision() == BELOW_0024


def test_one_declaration_document_survives_a_full_upgrade_head(
    isolated_migration_db: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The documented workflow is `alembic upgrade head` with ONE mounted file.

    It used to reject its own recovered rows. A declaration restoring an orphan
    to `slack` let 0022 commit; 0024 then loaded the SAME still-mounted document,
    did not find that approval in its own (non-Slack) refusal set, and refused it
    as "reconstructable" -- stopping the upgrade at 0023 with a document that had
    already done its job. The operator's only escape was to unmount the Secret
    between two revisions of a single `upgrade head`.

    An already-honored declaration is now recognized from its audit row and
    skipped: not refused, and not re-applied either, so a later revision can
    never overwrite an identity an earlier one established.
    """

    cfg = _at(BELOW_0022)
    orphan = _seed_approval(reply_channel="nobody@example.test", summary="no binding")
    _write_declarations(
        tmp_path,
        monkeypatch,
        [
            _declaration(
                orphan,
                reply_kind="slack",
                reply_adapter=None,
                reason="raised in #ops before the workspace was disconnected",
            )
        ],
    )

    command.upgrade(cfg, "head")

    # 0061 backfills the default identity onto every Slack reply.
    assert _reply_identity(orphan) == ("slack", "default")
    # Honored exactly once, by 0022, and never re-applied by a later revision.
    honored = [r for r in _audit_rows(orphan) if r.action == HONORED_ACTION]
    assert len(honored) == 1, honored
    assert honored[0].evidence["revision"] == REVISION_0022


# ==========================================================================
# The historical edits stay idempotent. Ruling 1.
# ==========================================================================


def test_the_full_upgrade_still_reaches_head_from_an_empty_database(
    isolated_migration_db: None,
) -> None:
    """The edits to two already-applied revisions take no new decision.

    This is the test that earns the right to touch 0022 and 0024 at all: an
    empty database walks the whole graph, fence and declaration arms included,
    and lands on head.
    """

    cfg = _alembic_config()
    command.upgrade(cfg, "head")
    assert "reply_kind" in _approvals_columns()


def test_0022_and_0024_still_apply_from_the_pre_0022_state(
    isolated_migration_db: None,
) -> None:
    """The same, seeded: a real installation sitting below 0022 upgrades through
    both edited revisions with no declaration file present, and the fence is a
    no-op nobody notices."""

    cfg = _at(BELOW_0022)
    _seed_binding("slack-agent", "slack", "C0EXAMPLE1")
    approval = _seed_approval(reply_channel="C0EXAMPLE1")

    command.upgrade(cfg, REVISION_0022)
    assert _reply_identity(approval) == ("slack", None)
    command.upgrade(cfg, REVISION_0024)
    assert _reply_identity(approval) == ("slack", None)
    command.upgrade(cfg, "head")
    # 0061 backfills the default identity onto every Slack reply.
    assert _reply_identity(approval) == ("slack", "default")


# ==========================================================================
# The retained installation. AC1.
# ==========================================================================


def test_a_retained_installation_upgrades_with_its_pending_approvals_intact(
    isolated_migration_db: None,
) -> None:
    """AC1 end to end: upgrade, then resolve, on a database that was already live.

    A pending approval with a real audit trail is seeded at the pre-0022 state,
    the whole graph runs, and afterwards the row is still `pending`, still
    carries the reply identity it was raised on, still has every audit row it
    had, and is STILL RESOLVABLE. Resolution is driven through
    `POST /approvals/{id}/resolve` -- the same path
    `test_approvals.py` uses -- because "the row looks fine" is a schema claim
    and "a human can still decide it" is the behavioral one AC1 actually makes.

    No forced rejection anywhere in the flow: the approval is settled AFTER the
    upgrade, by a decision, not drained before it to get the migration through.
    """

    import os
    import time as _time

    from curie_api import approval_principal
    from curie_api.main import create_app
    from fastapi.testclient import TestClient

    cfg = _at(BELOW_0022)
    _seed_binding("slack-agent", "slack", "C1")
    approval = _seed_approval(reply_channel="C1", summary="Give ACME a 20% discount")
    audit_ids = [
        _seed_audit_row(approval, "created"),
        _seed_audit_row(approval, "card_posted"),
    ]

    command.upgrade(cfg, "head")

    # Pending, with its original reply identity (the default Slack app, which
    # 0061 names), and its history whole.
    assert _sql("SELECT status FROM curie.approvals WHERE id = :id", {"id": approval}) == [
        ("pending",)
    ]
    assert _reply_identity(approval) == ("slack", "default")
    assert [row.id for row in _audit_rows(approval)] == audit_ids

    stream = f"test:curie:runs:{uuid.uuid4().hex}"
    os.environ["RUNS_STREAM"] = stream
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            headers = {"X-API-Key": get_settings().api_key}
            resolved = client.post(
                f"/approvals/{approval}/resolve",
                json={"decision": "approved"},
                headers={
                    **headers,
                    # A dispatcher-attested click, exactly as `test_approvals`
                    # mints one: identity and channel come from the credential,
                    # never from the body.
                    "X-Curie-Approval-Principal": approval_principal.mint(
                        get_settings().approval_chat_attester_secret,
                        subject="U9",
                        kind="chat",
                        actor_channel="C1",
                        approval_id=str(approval),
                        scope=approval_principal.APPROVE_SCOPE,
                        exp=int(_time.time()) + 60,
                    ),
                },
            )
            assert resolved.status_code == 200, resolved.text
            assert resolved.json()["status"] == "approved"
    finally:
        os.environ.pop("RUNS_STREAM", None)
        get_settings.cache_clear()

    # And the decision appended to the history rather than replacing it.
    assert [row.id for row in _audit_rows(approval)][: len(audit_ids)] == audit_ids


def test_the_refusal_is_self_sufficient_for_a_blocked_installation(
    isolated_migration_db: None,
) -> None:
    """The failed Job log has to be the whole disposition, not a pointer to a
    command that cannot run.

    `schema_compat.json` sets `schema_min` to head, so the API that serves the
    identity report REFUSES TO START against a pre-head schema -- and an
    installation blocked at 0022 is by definition on one. Telling that operator
    to go and run the reporting command would be the circular workflow AC4
    forbids. So the refusal itself carries both rows' facts and a declaration
    document pre-populated with their ids, ready to fill in.
    """

    cfg = _at(BELOW_0022)
    ambiguous = _seed_unreconstructable()
    orphan = _seed_approval(reply_channel="nobody@example.test", summary="no binding")

    with pytest.raises(RuntimeError) as caught:
        command.upgrade(cfg, REVISION_0022)

    message = str(caught.value)
    # Both rows, with the facts an operator needs to recognize them.
    for approval_id, channel in (
        (ambiguous, "shared@example.test"),
        (orphan, "nobody@example.test"),
    ):
        assert str(approval_id) in message, message
        assert channel in message, message
    assert "pending" in message
    # ...and WHY each one could not be reconstructed, which is the field the
    # operator has to answer when they fill the document in.
    assert "no binding" in message, message
    assert "distinct kinds" in message, message

    # The skeleton is a real document in the shape `load_declarations` accepts,
    # naming exactly these two rows and nobody else, with the human's fields
    # left empty so an unedited paste-back is refused rather than honored.
    start = message.index('{\n  "declarations"')
    skeleton = json.loads(message[start : message.rindex("}") + 1])
    assert {entry["approval_id"] for entry in skeleton["declarations"]} == {
        str(ambiguous),
        str(orphan),
    }
    for entry in skeleton["declarations"]:
        assert set(entry) == {"approval_id", "reply_kind", "reply_adapter", "actor", "reason"}
        assert entry["reply_kind"] == entry["actor"] == entry["reason"] == ""

    # And it names the transport the operator hands the completed document to.
    assert DECLARATIONS_ENV in message, message

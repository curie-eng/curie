"""Keep new-only automatic retries out of an older dispatcher's claim (ADR 0269).

An older dispatcher claims a waiting request whose waiting deadline is ahead
and whose ``dispatch_not_before`` has passed; it reads neither ``blocked_on``
nor ``execution_kind``. Two kinds of automatic successor must never run on it:
a resource gated retry, which it would start without checking the gate, and a
publication recovery, which it would start as an ordinary implementation
attempt with a paid model call.

``execution_requests`` gains ``guarded_dispatch_not_before``, the dispatch
time only this release's dispatcher reads. While it is set,
``dispatch_not_before`` holds the waiting deadline, so an older claim can never
match the row; the current claim reads the guarded time instead. A CHECK
states that invariant. A BEFORE INSERT OR UPDATE trigger keeps it for a writer
that does not know the column, such as an older release's deferral or lapsed
acquisition sweep after a rollback: a changed ``dispatch_not_before`` on a
guarded row moves into the guarded time, and ``dispatch_not_before`` is reset
to the waiting deadline. A partial index serves the claim's due order.

Existing rows keep NULL and are unchanged. The downgrade drops the trigger,
CHECK, index and column; a guarded row then keeps its deadline as its dispatch
time and expires there unclaimed, the safe direction.

Revision ID: 0182
Revises: 0179
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0182"
down_revision: str = "0179"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"


def upgrade() -> None:
    op.execute(
        f"""
        ALTER TABLE {S}.execution_requests
          ADD COLUMN guarded_dispatch_not_before timestamptz,
          ADD CONSTRAINT execution_requests_guarded_dispatch_ck CHECK (
            guarded_dispatch_not_before IS NULL
            OR (wait_deadline IS NOT NULL AND dispatch_not_before >= wait_deadline))
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {S}.guard_execution_requests_dispatch_due()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.guarded_dispatch_not_before IS NULL OR NEW.wait_deadline IS NULL THEN
                RETURN NEW;
            END IF;
            IF TG_OP = 'UPDATE'
               AND NEW.dispatch_not_before IS DISTINCT FROM OLD.dispatch_not_before
               AND NEW.guarded_dispatch_not_before
                   IS NOT DISTINCT FROM OLD.guarded_dispatch_not_before THEN
                -- A writer that does not know the guarded time rescheduled
                -- the row; its time is the guarded one.
                NEW.guarded_dispatch_not_before := NEW.dispatch_not_before;
            END IF;
            NEW.dispatch_not_before := NEW.wait_deadline;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER execution_requests_guarded_dispatch
        BEFORE INSERT OR UPDATE ON {S}.execution_requests
        FOR EACH ROW
        EXECUTE FUNCTION {S}.guard_execution_requests_dispatch_due()
        """
    )
    op.execute(
        f"""
        CREATE INDEX ix_execution_requests_guarded_dispatch_due
        ON {S}.execution_requests (guarded_dispatch_not_before)
        WHERE status = 'waiting' AND guarded_dispatch_not_before IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute(f"DROP INDEX {S}.ix_execution_requests_guarded_dispatch_due")
    op.execute(f"DROP TRIGGER execution_requests_guarded_dispatch ON {S}.execution_requests")
    op.execute(f"DROP FUNCTION {S}.guard_execution_requests_dispatch_due()")
    op.drop_constraint(
        "execution_requests_guarded_dispatch_ck", "execution_requests", schema=S, type_="check"
    )
    op.drop_column("execution_requests", "guarded_dispatch_not_before", schema=S)

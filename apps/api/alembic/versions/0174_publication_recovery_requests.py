"""Typed publication recovery requests (#5429, ADR 0269).

``execution_requests`` gains ``execution_kind``: every existing and ordinary
request is an ``implementation``; a ``publication_recovery`` request is an
automatic successor admitted to verify and publish one retained publication
candidate without another model run. Such a request names its candidate by id
and content digest, both set exactly when the kind is ``publication_recovery``.
A recovery request completes by a pull request and is always an automatic
successor. A trigger refuses any update of the three columns, so the
candidate identity fixed at admission never changes.

The downgrade refuses before any DDL while a recovery request exists, because
dropping the kind would turn it into an implementation request that reruns the
model.

Revision ID: 0174
Revises: 0173
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0174"
down_revision: str = "0173"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"

# A bounded wait for the table the downgrade guard reads, so a busy table
# refuses the downgrade rather than blocking writers behind it indefinitely.
LOCK_TIMEOUT_MS = 15000

COLUMNS = ("execution_kind", "recovery_candidate_id", "recovery_candidate_sha256")
CHECKS = (
    "execution_requests_execution_kind_ck",
    "execution_requests_recovery_candidate_pair_ck",
    "execution_requests_recovery_candidate_kind_ck",
    "execution_requests_recovery_shape_ck",
    "execution_requests_recovery_candidate_sha_ck",
)


def upgrade() -> None:
    op.execute(
        f"""
        ALTER TABLE {S}.execution_requests
          ADD COLUMN execution_kind text NOT NULL DEFAULT 'implementation',
          ADD COLUMN recovery_candidate_id uuid,
          ADD COLUMN recovery_candidate_sha256 varchar(64)
        """
    )
    op.execute(
        f"""
        ALTER TABLE {S}.execution_requests
          ADD CONSTRAINT execution_requests_execution_kind_ck CHECK (
            execution_kind IN ('implementation', 'publication_recovery')),
          ADD CONSTRAINT execution_requests_recovery_candidate_pair_ck CHECK (
            (recovery_candidate_id IS NULL) = (recovery_candidate_sha256 IS NULL)),
          ADD CONSTRAINT execution_requests_recovery_candidate_kind_ck CHECK (
            (execution_kind = 'publication_recovery') = (recovery_candidate_id IS NOT NULL)),
          ADD CONSTRAINT execution_requests_recovery_shape_ck CHECK (
            execution_kind <> 'publication_recovery'
            OR (completion_contract = 'implementation' AND retry_of_request_id IS NOT NULL)),
          ADD CONSTRAINT execution_requests_recovery_candidate_sha_ck CHECK (
            recovery_candidate_sha256 IS NULL
            OR recovery_candidate_sha256 ~ '^[0-9a-f]{{64}}$')
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {S}.enforce_execution_requests_recovery_identity()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.execution_kind IS DISTINCT FROM OLD.execution_kind
               OR NEW.recovery_candidate_id IS DISTINCT FROM OLD.recovery_candidate_id
               OR NEW.recovery_candidate_sha256
                  IS DISTINCT FROM OLD.recovery_candidate_sha256 THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'execution request recovery identity is immutable';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER execution_requests_recovery_identity
        BEFORE UPDATE OF execution_kind, recovery_candidate_id, recovery_candidate_sha256
        ON {S}.execution_requests
        FOR EACH ROW
        EXECUTE FUNCTION {S}.enforce_execution_requests_recovery_identity()
        """
    )


def downgrade() -> None:
    # Held from the check to the drop, so no recovery request lands in between.
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    op.execute(sa.text(f"LOCK TABLE {S}.execution_requests IN ACCESS EXCLUSIVE MODE"))
    present = (
        op.get_bind()
        .exec_driver_sql(
            f"SELECT EXISTS (SELECT 1 FROM {S}.execution_requests "
            "WHERE execution_kind = 'publication_recovery')"
        )
        .scalar_one()
    )
    if present:
        raise RuntimeError("publication_recovery_requests_present")
    op.execute(f"DROP TRIGGER execution_requests_recovery_identity ON {S}.execution_requests")
    op.execute(f"DROP FUNCTION {S}.enforce_execution_requests_recovery_identity()")
    for name in CHECKS:
        op.drop_constraint(name, "execution_requests", schema=S, type_="check")
    for column in COLUMNS:
        op.drop_column("execution_requests", column, schema=S)

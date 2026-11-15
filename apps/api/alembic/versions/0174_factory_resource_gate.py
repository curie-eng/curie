"""Resource gated automatic retries for factory executions (#5427, ADR 0269).

``execution_requests`` gains two nullable JSONB columns. ``delivered_limits``
is the record a start writes of the limits actually delivered to that attempt,
with the runtime epoch it started; a scratch exhaustion adds the volume it
filled. ``resource_gate`` is what an automatic successor of a limit failure
waits for: the failed dimension and the limit its predecessor had. Each is a
JSON object when present. Existing rows keep both NULL: a request started
before this revision has no record, so a limit failure of it admits no gated
successor.

The revision only adds. The downgrade refuses before any DDL while any
request carries a gate, because dropping it would leave that successor
blocked on a condition nothing can release. Records alone do not refuse it:
losing one only means a later limit failure admits no successor, the safe
direction.

Revision ID: 0174
Revises: 0172
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0174"
down_revision: str = "0172"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
S = "curie"

# A bounded wait for the table the downgrade guard reads, so a busy table
# refuses the downgrade rather than blocking writers behind it indefinitely.
LOCK_TIMEOUT_MS = 15000

REQUEST_COLUMNS = ("delivered_limits", "resource_gate")
REQUEST_CHECKS = (
    "execution_requests_delivered_limits_ck",
    "execution_requests_resource_gate_ck",
)


def upgrade() -> None:
    op.execute(
        f"""
        ALTER TABLE {S}.execution_requests
          ADD COLUMN delivered_limits jsonb,
          ADD COLUMN resource_gate jsonb
        """
    )
    op.execute(
        f"""
        ALTER TABLE {S}.execution_requests
          ADD CONSTRAINT execution_requests_delivered_limits_ck CHECK (
            delivered_limits IS NULL OR jsonb_typeof(delivered_limits) = 'object'),
          ADD CONSTRAINT execution_requests_resource_gate_ck CHECK (
            resource_gate IS NULL OR jsonb_typeof(resource_gate) = 'object')
        """
    )


def downgrade() -> None:
    # Held from the check to the drop, so no gate lands in between.
    op.execute(sa.text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT_MS}ms'"))
    op.execute(sa.text(f"LOCK TABLE {S}.execution_requests IN ACCESS EXCLUSIVE MODE"))
    present = (
        op.get_bind()
        .exec_driver_sql(
            f"SELECT EXISTS (SELECT 1 FROM {S}.execution_requests WHERE resource_gate IS NOT NULL)"
        )
        .scalar_one()
    )
    if present:
        raise RuntimeError("factory_resource_gate_state_present")
    for name in REQUEST_CHECKS:
        op.drop_constraint(name, "execution_requests", schema=S, type_="check")
    for column in REQUEST_COLUMNS:
        op.drop_column("execution_requests", column, schema=S)

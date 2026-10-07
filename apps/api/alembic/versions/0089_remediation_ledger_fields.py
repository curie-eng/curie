"""Authority, actor and remediation provenance on the ledger (AUTOMATED-REMEDIATION-14)

Revision ID: 0089
Revises: 0088
Create Date: 2026-10-07

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database).

``agent_actions`` gains ``delivery_event_id`` (the protected delivery a
remediation was nominated from), ``nomination_id`` (deliberately not a foreign
key, like ``gate_approval_id``: the record of what was done outlives the
nomination row), ``verification_outcome`` and ``verified_at`` (written by
verification), and ``actor_kind``. ``action_audit_entries`` gains
``actor_kind``. Every new column is nullable with no default, so existing rows
read NULL and nothing is backfilled.

Check constraints close ``authority_kind`` on ``agent_actions`` and
``action_executions`` to ``undo_ruling``, ``capability_probe``, ``policy``,
``approval`` and ``qualification`` (or NULL where the column allows it),
``actor_kind`` on both ledger tables to ``model_turn``, ``policy``,
``approval`` and ``undo_ruling``, and ``verification_outcome`` to the four
outcomes. Every value the product wrote before this revision is inside those
sets, so adding them validates existing rows without rewriting any.

The downgrade drops the checks and the new columns only.

@spec AUTOMATED-REMEDIATION-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0089"
down_revision: str | None = "0088"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
ACTIONS = "agent_actions"
AUDIT = "action_audit_entries"
EXECUTIONS = "action_executions"

AUTHORITY_KINDS = "('undo_ruling', 'capability_probe', 'policy', 'approval', 'qualification')"
ACTOR_KINDS = "('model_turn', 'policy', 'approval', 'undo_ruling')"
OUTCOMES = "('verified', 'not-recovered', 'verifier-unavailable', 'superseded')"

# (table, constraint name, condition)
CHECKS: tuple[tuple[str, str, str], ...] = (
    (
        ACTIONS,
        "agent_actions_authority_kind_ck",
        f"authority_kind IS NULL OR authority_kind IN {AUTHORITY_KINDS}",
    ),
    (ACTIONS, "agent_actions_actor_kind_ck", f"actor_kind IS NULL OR actor_kind IN {ACTOR_KINDS}"),
    (
        ACTIONS,
        "agent_actions_verification_outcome_ck",
        f"verification_outcome IS NULL OR verification_outcome IN {OUTCOMES}",
    ),
    (EXECUTIONS, "action_executions_authority_kind_ck", f"authority_kind IN {AUTHORITY_KINDS}"),
    (
        AUDIT,
        "action_audit_entries_actor_kind_ck",
        f"actor_kind IS NULL OR actor_kind IN {ACTOR_KINDS}",
    ),
)


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-14."""
    op.add_column(ACTIONS, sa.Column("delivery_event_id", sa.String(256)), schema=SCHEMA)
    op.add_column(
        ACTIONS, sa.Column("nomination_id", postgresql.UUID(as_uuid=True)), schema=SCHEMA
    )
    op.add_column(ACTIONS, sa.Column("verification_outcome", sa.Text()), schema=SCHEMA)
    op.add_column(ACTIONS, sa.Column("verified_at", sa.DateTime(timezone=True)), schema=SCHEMA)
    op.add_column(ACTIONS, sa.Column("actor_kind", sa.Text()), schema=SCHEMA)
    op.add_column(AUDIT, sa.Column("actor_kind", sa.Text()), schema=SCHEMA)
    for table, name, condition in CHECKS:
        op.create_check_constraint(name, table, condition, schema=SCHEMA)


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-14."""
    for table, name, _ in reversed(CHECKS):
        op.drop_constraint(name, table, type_="check", schema=SCHEMA)
    op.drop_column(AUDIT, "actor_kind", schema=SCHEMA)
    for column in (
        "actor_kind",
        "verified_at",
        "verification_outcome",
        "nomination_id",
        "delivery_event_id",
    ):
        op.drop_column(ACTIONS, column, schema=SCHEMA)

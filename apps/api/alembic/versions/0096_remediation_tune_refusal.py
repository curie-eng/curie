"""Remediation tune refusal (AUTOMATED-REMEDIATION-25)

Revision ID: 0096
Revises: 0095
Create Date: 2026-10-07

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database).

An approved alert rule tuning request executes nothing (maintainer ruling,
2026-10-07): every nomination of it ends ``refused`` with
``tune_execution_not_automated``. ``remediation_nominations_refusal_ck`` is
widened to accept that code, so it holds exactly the frozen
``nomination_refusals`` of ``tests/vectors/remediation-codes.json``; any other
code is still refused.

The downgrade keeps each such nomination's record in the shape the narrower
check allows: ``finished`` with the code in ``execution_code`` (revision 0091)
and no refusal code, then restores the narrower check.

@spec AUTOMATED-REMEDIATION-25
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0096"
down_revision: str | None = "0095"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
NOMINATIONS = "remediation_nominations"
REFUSAL_CHECK = "remediation_nominations_refusal_ck"
TUNE_REFUSAL = "tune_execution_not_automated"
REFUSALS_BEFORE = (
    "refusal_code IS NULL OR refusal_code IN ('nomination_malformed', 'unknown_action', "
    "'nomination_duplicate', 'arguments_schema_mismatch', 'agent_stopped', "
    "'reply_surface_unavailable')"
)
REFUSALS_WITH_TUNE = (
    "refusal_code IS NULL OR refusal_code IN ('nomination_malformed', 'unknown_action', "
    "'nomination_duplicate', 'arguments_schema_mismatch', 'agent_stopped', "
    "'reply_surface_unavailable', 'tune_execution_not_automated')"
)


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-25."""
    op.drop_constraint(REFUSAL_CHECK, NOMINATIONS, type_="check", schema=SCHEMA)
    op.create_check_constraint(REFUSAL_CHECK, NOMINATIONS, REFUSALS_WITH_TUNE, schema=SCHEMA)


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-25."""
    op.execute(
        f"UPDATE {SCHEMA}.{NOMINATIONS} SET state = 'finished', refusal_code = NULL, "
        f"execution_code = '{TUNE_REFUSAL}' WHERE refusal_code = '{TUNE_REFUSAL}'"
    )
    op.drop_constraint(REFUSAL_CHECK, NOMINATIONS, type_="check", schema=SCHEMA)
    op.create_check_constraint(REFUSAL_CHECK, NOMINATIONS, REFUSALS_BEFORE, schema=SCHEMA)

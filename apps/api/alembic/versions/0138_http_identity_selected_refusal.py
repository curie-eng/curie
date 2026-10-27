"""Remediation nomination refusal for an HTTP-selected approval scope

Revision ID: 0138
Revises: 0137
Create Date: 2026-10-27

Hand-written and additive (ADR 0117 found autogenerate unsafe against the
shared database).

The internal remediation approval writer refuses an HTTP-selected approval
scope before insertion, so such a nomination could never be raised and was
selected again on every reconcile pass. It now ends once, ``refused`` with
``http_identity_selected``. ``remediation_nominations_refusal_ck`` is widened
to accept that code, so it holds exactly the frozen ``nomination_refusals`` of
``tests/vectors/remediation-codes.json``; any other code is still refused.

The downgrade returns each such nomination to the owed state it was ended
from (``approval_requested``, its reason kept, no approval) and restores the
narrower check.

@spec AUTOMATED-REMEDIATION-15
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0138"
down_revision: str | None = "0137"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
NOMINATIONS = "remediation_nominations"
REFUSAL_CHECK = "remediation_nominations_refusal_ck"
SELECTED_REFUSAL = "http_identity_selected"
REFUSALS_BEFORE = (
    "refusal_code IS NULL OR refusal_code IN ('nomination_malformed', 'unknown_action', "
    "'nomination_duplicate', 'arguments_schema_mismatch', 'agent_stopped', "
    "'reply_surface_unavailable', 'tune_execution_not_automated')"
)
REFUSALS_WITH_SELECTED = (
    "refusal_code IS NULL OR refusal_code IN ('nomination_malformed', 'unknown_action', "
    "'nomination_duplicate', 'arguments_schema_mismatch', 'agent_stopped', "
    "'reply_surface_unavailable', 'tune_execution_not_automated', 'http_identity_selected')"
)


def upgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-15."""
    op.drop_constraint(REFUSAL_CHECK, NOMINATIONS, type_="check", schema=SCHEMA)
    op.create_check_constraint(REFUSAL_CHECK, NOMINATIONS, REFUSALS_WITH_SELECTED, schema=SCHEMA)


def downgrade() -> None:
    """@spec AUTOMATED-REMEDIATION-15."""
    op.execute(
        f"UPDATE {SCHEMA}.{NOMINATIONS} SET state = 'approval_requested', refusal_code = NULL "
        f"WHERE refusal_code = '{SELECTED_REFUSAL}'"
    )
    op.drop_constraint(REFUSAL_CHECK, NOMINATIONS, type_="check", schema=SCHEMA)
    op.create_check_constraint(REFUSAL_CHECK, NOMINATIONS, REFUSALS_BEFORE, schema=SCHEMA)

"""Pin generic input compatibility declarations and admitted receipts.

Revision ID: 0109
Revises: 0108
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0109"
down_revision = "0108"
branch_labels = None
depends_on = None

# Frozen storage shape; the ORM carries the same constraint. CASE guards JSONB
# object operations from scalar input, and IS TRUE rejects missing/null fields.
RECEIPT_CHECK = (
    "input_contract_receipt IS NULL OR CASE "
    "WHEN jsonb_typeof(input_contract_receipt) = 'object' THEN ("
    "input_contract_receipt - ARRAY['interface','contract','deployment_id',"
    "'version_id','bundle_sha256','body_sha256']::text[] = '{}'::jsonb "
    "AND jsonb_typeof(input_contract_receipt->'interface') = 'string' "
    "AND jsonb_typeof(input_contract_receipt->'contract') = 'string' "
    "AND jsonb_typeof(input_contract_receipt->'deployment_id') = 'string' "
    "AND jsonb_typeof(input_contract_receipt->'version_id') = 'string' "
    "AND jsonb_typeof(input_contract_receipt->'bundle_sha256') = 'string' "
    "AND jsonb_typeof(input_contract_receipt->'body_sha256') = 'string'"
    ") IS TRUE ELSE FALSE END"
)


def upgrade() -> None:
    op.add_column(
        "agent_versions",
        sa.Column("input_contracts", postgresql.JSONB(), nullable=True),
        schema="curie",
    )
    op.add_column(
        "execution_requests",
        sa.Column("input_contract_receipt", postgresql.JSONB(), nullable=True),
        schema="curie",
    )
    op.create_check_constraint(
        "execution_requests_input_contract_receipt_ck",
        "execution_requests",
        RECEIPT_CHECK,
        schema="curie",
    )


def downgrade() -> None:
    op.drop_constraint(
        "execution_requests_input_contract_receipt_ck",
        "execution_requests",
        schema="curie",
        type_="check",
    )
    op.drop_column("execution_requests", "input_contract_receipt", schema="curie")
    op.drop_column("agent_versions", "input_contracts", schema="curie")

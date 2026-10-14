"""Admit authenticated test-driver approval audit entries (ADR 0202).

Revision ID: 0098
Revises: 0097
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0098"
down_revision: str | None = "0097"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "approval_audit_entries"
CONSTRAINT = "approval_audit_principal_kind_ck"


def _constraint(kinds: str) -> None:
    op.drop_constraint(CONSTRAINT, TABLE, schema=SCHEMA, type_="check")
    op.create_check_constraint(
        CONSTRAINT, TABLE, f"principal_kind IS NULL OR principal_kind IN ({kinds})", schema=SCHEMA
    )


def upgrade() -> None:
    _constraint("'chat', 'console', 'operator', 'adapter', 'platform', 'test_driver'")


def downgrade() -> None:
    # Authenticated append-only history must not be relabelled or discarded.
    # Downgrading with test-driver rows is refused by PostgreSQL's constraint.
    _constraint("'chat', 'console', 'operator', 'adapter', 'platform'")

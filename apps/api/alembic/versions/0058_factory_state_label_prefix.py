"""Widen the factory state label check (#3221).

Expand. ``applied_label`` may be NULL, empty, one of the four legacy
``curie:*`` names, or one of the four ``curie-factory:*`` names. Stored values
are not rewritten: a legacy name left in place is what makes the next
reconciler pass replace the GitHub label.

Downgrade restores the 0057 check and refuses while any row stores a new name.

Revision ID: 0058
Revises: 0057
Create Date: 2026-09-25
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0058"
down_revision: str | None = "0057"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "factory_terminal_notices"
_LEGACY_LABELS = "('', 'curie:queued', 'curie:running', 'curie:pr-open', 'curie:needs-human')"
_LABELS = (
    "('', 'curie:queued', 'curie:running', 'curie:pr-open', 'curie:needs-human', "
    "'curie-factory:queued', 'curie-factory:running', 'curie-factory:pr-open', "
    "'curie-factory:needs-human')"
)
_PREDICATE = f"applied_label IS NULL OR applied_label IN {_LABELS}"
_LEGACY_PREDICATE = f"applied_label IS NULL OR applied_label IN {_LEGACY_LABELS}"
_NEW_LABELS = (
    "'curie-factory:queued', 'curie-factory:running', "
    "'curie-factory:pr-open', 'curie-factory:needs-human'"
)


def upgrade() -> None:
    op.drop_constraint(f"{TABLE}_applied_label_ck", TABLE, schema=SCHEMA)
    op.create_check_constraint(
        f"{TABLE}_applied_label_ck",
        TABLE,
        _PREDICATE,
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {SCHEMA}.{TABLE}
                WHERE applied_label IN ({_NEW_LABELS})
            ) THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = '0058 downgrade refused: a status comment stores '
                              'a curie-factory state label, which 0057 cannot hold';
            END IF;
        END
        $$
        """
    )
    op.drop_constraint(f"{TABLE}_applied_label_ck", TABLE, schema=SCHEMA)
    op.create_check_constraint(
        f"{TABLE}_applied_label_ck",
        TABLE,
        _LEGACY_PREDICATE,
        schema=SCHEMA,
    )

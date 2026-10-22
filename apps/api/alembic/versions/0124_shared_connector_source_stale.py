"""Persist shared connector source staleness (ADR 0239).

A platform binding records the version of the Kubernetes Secret it was imported
from. The resolver cannot read that Secret and holds no Kubernetes credential,
so it cannot compare versions itself ("Static sources" 1). The API check, which
is handed the Secret's current uid and resource version by the operator, is the
only place a mismatch is seen. Until now it reported the mismatch and stored
nothing, so a resolve after the check still served the old credential.

``source_stale boolean NOT NULL DEFAULT false`` on
``curie.shared_connector_bindings`` closes that gap. The API check sets it on a
mismatch and reports ``source_stale`` for as long as it is set; only an explicit
reimport clears it. The resolver reads the flag with the SELECT grant it already
has and refuses a stale binding. The application role gains no new grant:
``lock_guard`` stays the only column it may update, so it can neither set nor
clear the flag.

``shared_connector_bindings_source_stale_ck`` (``NOT source_stale OR custody =
'platform'``) keeps connector custody, which holds no source, from ever
carrying the flag.

Expand only: a constant-default, NOT NULL column add is metadata-only on
PostgreSQL 16, and existing rows read false. The downgrade drops the constraint,
then the column.

Revision ID: 0124
Revises: 0123
Create Date: 2026-10-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0124"
down_revision: str | None = "0123"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
BINDINGS = "shared_connector_bindings"
SOURCE_STALE_CK = "shared_connector_bindings_source_stale_ck"


def upgrade() -> None:
    op.add_column(
        BINDINGS,
        sa.Column("source_stale", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        SOURCE_STALE_CK,
        BINDINGS,
        "NOT source_stale OR custody = 'platform'",
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_constraint(SOURCE_STALE_CK, BINDINGS, schema=SCHEMA, type_="check")
    op.drop_column(BINDINGS, "source_stale", schema=SCHEMA)

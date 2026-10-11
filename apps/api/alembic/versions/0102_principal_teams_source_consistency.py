"""principal_teams.source must agree with its team's source (#3002).

Follow-up from the #2907 review (epic #2917, ADR 0166 step 2). Migration 0057
gave ``principal_teams`` its own ``source`` check, independent of
``teams.source``: nothing stopped a ``curie_managed`` membership row from
sitting in an ``idp_group`` team, contradicting the rule that the IdP alone is
the system of record for an ``idp_group`` team's membership (and conversely for
a ``curie_managed`` team). A per-row ``CHECK`` cannot read another table, so
this is a ``BEFORE INSERT OR UPDATE`` trigger instead: it looks up the
referenced team's ``source`` and rejects a mismatch. A membership whose team
cannot be found yet is left to the existing composite foreign keys, which run
after this trigger and reject it on their own terms.

Revision ID: 0102
Revises: 0101
Create Date: 2026-10-07
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0102"
down_revision: str | None = "0101"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"

_FUNCTION = f"""
CREATE OR REPLACE FUNCTION {SCHEMA}.enforce_principal_teams_source_match()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    team_source text;
BEGIN
    SELECT source INTO team_source
    FROM {SCHEMA}.teams
    WHERE id = NEW.team_id AND tenant_id = NEW.tenant_id;

    -- An out-of-enum NEW.source (not itself idp_group/curie_managed) is left
    -- to principal_teams_source_ck, which runs after this trigger; comparing
    -- it here would misreport a plain bad-value insert as a team mismatch.
    IF FOUND
       AND NEW.source IN ('idp_group', 'curie_managed')
       AND team_source IS DISTINCT FROM NEW.source THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'principal_teams.source must match its team''s source';
    END IF;

    RETURN NEW;
END;
$$
"""

_TRIGGER = f"""
CREATE TRIGGER principal_teams_source_matches_team
    BEFORE INSERT OR UPDATE OF source, team_id, tenant_id
    ON {SCHEMA}.principal_teams
    FOR EACH ROW
    EXECUTE FUNCTION {SCHEMA}.enforce_principal_teams_source_match()
"""


def upgrade() -> None:
    op.execute(_FUNCTION)
    op.execute(_TRIGGER)


def downgrade() -> None:
    op.execute(
        f"DROP TRIGGER principal_teams_source_matches_team "
        f"ON {SCHEMA}.principal_teams"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.enforce_principal_teams_source_match()")

"""Allow the enrollment_inactive factory intake notice (#5713).

A linked label actor with no active personal enrollment is a different refusal
from an unlinked actor. The intake notice CHECK gains that reason. Downgrade
restores the previous list and refuses while any enrollment_inactive notice
exists. factory_slack_refusal_posts stores the reason in its payload and has
no reason CHECK, so this revision does not alter that table.

Expand only: a release that never writes enrollment_inactive still accepts
every reason it writes. This release writes the new reason, so it cannot
serve a database below 0177.

Revision ID: 0177
Revises: 0176
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0177"
down_revision: str = "0176"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

S = "curie"
CHECK = "factory_intake_notices_reason_ck"
TABLE = "factory_intake_notices"
PREVIOUS = (
    "actor_unverifiable",
    "actor_bot",
    "actor_own_app",
    "actor_no_permission",
    "personal_owner_required",
    "binding_ambiguous",
    "publication_completed",
    "installation_changed",
)
CURRENT = PREVIOUS[:5] + ("enrollment_inactive",) + PREVIOUS[5:]


def _clause(reasons: tuple[str, ...]) -> str:
    return "reason IN (" + ", ".join(repr(reason) for reason in reasons) + ")"


def upgrade() -> None:
    op.drop_constraint(CHECK, TABLE, schema=S, type_="check")
    op.create_check_constraint(CHECK, TABLE, _clause(CURRENT), schema=S)


def downgrade() -> None:
    count = op.get_bind().scalar(
        sa.text(
            "SELECT count(*) FROM curie.factory_intake_notices "
            "WHERE reason = 'enrollment_inactive'"
        )
    )
    if count:
        raise RuntimeError(
            "cannot downgrade while enrollment_inactive intake notices exist"
        )
    op.drop_constraint(CHECK, TABLE, schema=S, type_="check")
    op.create_check_constraint(CHECK, TABLE, _clause(PREVIOUS), schema=S)

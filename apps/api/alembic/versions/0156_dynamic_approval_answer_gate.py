"""Gate a dynamic approval's answer on the enforcing answer owner. @spec REQUEST-APPROVAL-5.

Revision ID: 0156
Revises: 0153

ADR 0271 decision 5. Revision 0153 added ``approvals_dynamic_unapproved``, a
BEFORE UPDATE trigger backed by ``enforce_dynamic_approval_unapproved()``, that
refused every update approving a dynamic approval "until its selected approver
set is enforced at answer time". The HTTP answer owner now enforces that set,
the ceiling and separation, so this revision keeps the trigger and replaces
only the function body: approving a dynamic row is refused, with the same
error, unless the same transaction set ``curie.approval_selection_enforced``
to exactly ``v1`` (``set_config(..., true)``, transaction-local). Only the
answer owner sets it, in its final commit after those checks pass
(``identity/http_approval_answer.py::SELECTION_ENFORCED``).

The revision is an expand. An API that predates answer enforcement never sets
the gate, so during a rolling upgrade or after an application rollback it still
cannot approve a dynamic row, through the route's static approvers or
otherwise. Rejection, expiry and ordinary rows never needed the gate. Every
other 0153 object stays: the mode and snapshot CHECKs, the nonce index, the
selector-flag CHECK, the INSERT route-lock trigger and the creation
``FOR UPDATE`` lock.

The downgrade restores the 0153 function body verbatim, so no transaction can
approve a dynamic row again. A row approved meanwhile keeps its status: the
trigger fires only on later updates.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0156"
down_revision: str = "0154"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
# The gate the enforcing answer owner sets; frozen here, so a later edit of
# ``http_approval_answer.SELECTION_ENFORCED`` cannot rewrite this revision.
GATE_SETTING = "curie.approval_selection_enforced"
GATE_VALUE = "v1"
_GATED_FUNCTION = f"""
CREATE OR REPLACE FUNCTION {SCHEMA}.enforce_dynamic_approval_unapproved()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'approved'
       AND (OLD.selection_mode = 'dynamic' OR NEW.selection_mode = 'dynamic')
       AND current_setting('{GATE_SETTING}', true) IS DISTINCT FROM '{GATE_VALUE}' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'dynamic_selection_unavailable: a dynamic approval cannot be approved '
                || 'until its selected approver set is enforced at answer time';
    END IF;
    RETURN NEW;
END;
$$
"""
# A frozen copy of 0153's ``_UNAPPROVED_FUNCTION`` body, byte for byte.
_UNAPPROVED_FUNCTION = f"""
CREATE OR REPLACE FUNCTION {SCHEMA}.enforce_dynamic_approval_unapproved()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'approved'
       AND (OLD.selection_mode = 'dynamic' OR NEW.selection_mode = 'dynamic') THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'dynamic_selection_unavailable: a dynamic approval cannot be approved '
                || 'until its selected approver set is enforced at answer time';
    END IF;
    RETURN NEW;
END;
$$
"""


def upgrade() -> None:
    """Admit an approve only under the answer owner's gate. @spec REQUEST-APPROVAL-5."""
    op.execute(_GATED_FUNCTION)


def downgrade() -> None:
    """Restore the 0153 interlock body verbatim. @spec REQUEST-APPROVAL-5."""
    op.execute(_UNAPPROVED_FUNCTION)

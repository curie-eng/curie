"""The immutable approval selection snapshot. @spec REQUEST-APPROVAL-4.

Revision ID: 0153
Revises: 0152

ADR 0271 decision 4. Every existing approval reads back ``ordinary`` with no
selection; only a dynamic approval created from a verified approver ticket
carries the requester, connector, tool, key id, nonce, argument digest and
selected approvers, all of them or none (a CHECK). The partial unique index
spends a ticket nonce once per agent. The checks are frozen copies of
``models.Approval``'s and ``models.Agent``'s, so a later model edit cannot
rewrite this revision.

The revision is an expand, so an older API keeps serving it during a rolling
upgrade and after an application rollback. Such a writer knows nothing of
selectors, so storage holds ADR 0271's invariants for every writer, whatever
its version (@spec REQUEST-APPROVAL-1/2/4):

1. ``agents_approver_ticket_requires_requester_identity_ck``: an agent whose
   route map carries an ``approver_ticket`` key keeps
   ``connector_requester_identity`` on, so an older flag writer cannot turn it
   off under a registration.
2. ``approvals_approver_ticket_route_lock``: the route lock both ways, on
   exactly the rows the two creation owners write (``purpose = 'session'``,
   the rows ``approver_tickets.check_tool_route`` judges). A registered
   pair's live name (``mcp__<connector>__<tool>``) is stored only on its
   owning route, an owning route stores only that pair and only as a dynamic
   row for it, and a top-level ``approval_ticket`` or a dynamic row (of any
   purpose) needs an owning route. So an older creation owner cannot record
   the pair on a looser route, routeless or as an ordinary row, another tool
   on the owning route, or a ticket on an unregistered route. A stored
   registration without a text connector and tool refuses every creation
   insert for its agent, like the application's fail-closed read.
3. ``approvals_dynamic_unapproved``: a dynamic approval never becomes
   ``approved`` until decision 5's answer authority replaces this trigger, so
   an older answer path cannot grant it through the route's static approvers.

The downgrade refuses while any dynamic approval or registration exists: it
would drop the snapshot and the spent nonce of each dynamic row, which would
then read back ordinary with its ticket reusable.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0153"
down_revision: str = "0152"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
_SNAPSHOT_COLUMNS = (
    "selection_requester",
    "selection_connector",
    "selection_tool",
    "selection_key_id",
    "selection_nonce",
    "selection_arguments_sha256",
    "selected_approvers",
)
_MODE_CHECK = "selection_mode IN ('ordinary', 'dynamic')"
_SNAPSHOT_CHECK = (
    "(selection_mode = 'dynamic' AND "
    + " AND ".join(f"{column} IS NOT NULL" for column in _SNAPSHOT_COLUMNS)
    + ") OR (selection_mode = 'ordinary' AND "
    + " AND ".join(f"{column} IS NULL" for column in _SNAPSHOT_COLUMNS)
    + ")"
)
# ``approver_tickets.has_selector`` in SQL: some route of an object route map
# carries an ``approver_ticket`` key, even a malformed or null one.
_HAS_SELECTOR = (
    "COALESCE(jsonb_path_exists(approval_routes, "
    "'strict $.* ? (@.type() == \"object\" && exists(@.approver_ticket))', '{}', true), false)"
)
_SELECTOR_FLAG_CHECK = f"connector_requester_identity OR NOT {_HAS_SELECTOR}"
_ROUTE_LOCK_FUNCTION = f"""
CREATE FUNCTION {SCHEMA}.enforce_approver_ticket_route_lock()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    stored_routes jsonb;
    entry record;
    registration jsonb;
    live text;
    owned boolean := false;
    created boolean;
BEGIN
    created := NEW.purpose = 'session';
    IF NEW.selection_mode = 'dynamic' AND NEW.status = 'approved' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'dynamic_selection_unavailable: a dynamic approval cannot be approved '
                || 'until its selected approver set is enforced at answer time';
    END IF;
    SELECT approval_routes INTO stored_routes FROM {SCHEMA}.agents WHERE id = NEW.agent_id;
    IF jsonb_typeof(stored_routes) = 'object' THEN
        FOR entry IN SELECT key, value FROM jsonb_each(stored_routes) LOOP
            CONTINUE WHEN jsonb_typeof(entry.value) IS DISTINCT FROM 'object'
                OR (entry.value -> 'approver_ticket') IS NULL;
            registration := entry.value -> 'approver_ticket';
            IF jsonb_typeof(registration -> 'connector') IS DISTINCT FROM 'string'
               OR jsonb_typeof(registration -> 'tool') IS DISTINCT FROM 'string' THEN
                CONTINUE WHEN NOT created;
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'approver_ticket_state_invalid: a stored approver_ticket '
                        || 'registration of this agent has no connector or tool';
            END IF;
            live := 'mcp__' || (registration ->> 'connector') || '__' || (registration ->> 'tool');
            IF NEW.route <> '' AND NEW.route = entry.key THEN
                owned := true;
                CONTINUE WHEN NOT created;
                IF NEW.granted_tool IS DISTINCT FROM live THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '23514',
                        MESSAGE = 'approval_ticket_route_mismatch: a route that registers an '
                            || 'approver_ticket records only its registered tool';
                END IF;
                IF NEW.selection_mode IS DISTINCT FROM 'dynamic'
                   OR NEW.selection_connector IS DISTINCT FROM registration ->> 'connector'
                   OR NEW.selection_tool IS DISTINCT FROM registration ->> 'tool' THEN
                    RAISE EXCEPTION USING
                        ERRCODE = '23514',
                        MESSAGE = 'approval_ticket_required: a registered approver_ticket tool '
                            || 'is recorded only as a dynamic approval for that tool';
                END IF;
            ELSIF created AND NEW.granted_tool = live THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    MESSAGE = 'approval_route_locked: a registered approver_ticket tool is '
                        || 'recorded only on its owning route';
            END IF;
        END LOOP;
    END IF;
    IF NOT owned AND (
        NEW.selection_mode = 'dynamic'
        OR (created AND (NEW.granted_arguments -> 'approval_ticket') IS NOT NULL)
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'approval_ticket_unregistered: a ticket or dynamic approval needs a '
                || 'route that registers an approver_ticket';
    END IF;
    RETURN NEW;
END;
$$
"""
_UNAPPROVED_FUNCTION = f"""
CREATE FUNCTION {SCHEMA}.enforce_dynamic_approval_unapproved()
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
    """@spec REQUEST-APPROVAL-4."""
    op.add_column(
        "approvals",
        sa.Column("selection_mode", sa.Text(), nullable=False, server_default="ordinary"),
        schema=SCHEMA,
    )
    op.add_column(
        "approvals",
        sa.Column("selection_requester", postgresql.JSONB(), nullable=True),
        schema=SCHEMA,
    )
    for column in (
        "selection_connector",
        "selection_tool",
        "selection_key_id",
        "selection_nonce",
        "selection_arguments_sha256",
    ):
        op.add_column("approvals", sa.Column(column, sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column(
        "approvals",
        sa.Column("selected_approvers", postgresql.JSONB(), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "approvals_selection_mode_ck",
        "approvals",
        _MODE_CHECK,
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "approvals_selection_snapshot_ck",
        "approvals",
        _SNAPSHOT_CHECK,
        schema=SCHEMA,
    )
    op.create_index(
        "approvals_selection_nonce_key",
        "approvals",
        ["agent_id", "selection_nonce"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("selection_nonce IS NOT NULL"),
    )
    op.create_check_constraint(
        "agents_approver_ticket_requires_requester_identity_ck",
        "agents",
        _SELECTOR_FLAG_CHECK,
        schema=SCHEMA,
    )
    op.execute(_ROUTE_LOCK_FUNCTION)
    op.execute(
        f"""
        CREATE TRIGGER approvals_approver_ticket_route_lock
        BEFORE INSERT ON {SCHEMA}.approvals
        FOR EACH ROW
        EXECUTE FUNCTION {SCHEMA}.enforce_approver_ticket_route_lock()
        """
    )
    op.execute(_UNAPPROVED_FUNCTION)
    op.execute(
        f"""
        CREATE TRIGGER approvals_dynamic_unapproved
        BEFORE UPDATE OF status, selection_mode ON {SCHEMA}.approvals
        FOR EACH ROW
        EXECUTE FUNCTION {SCHEMA}.enforce_dynamic_approval_unapproved()
        """
    )


def downgrade() -> None:
    """Refuse while selection state exists; otherwise drop the snapshot and guards.

    A dynamic approval's snapshot and spent nonce, and an agent's
    ``approver_ticket`` registration, exist only at this revision. Dropping
    them would turn each dynamic row ordinary with its ticket reusable, and
    leave a registration no older release enforces, so the downgrade refuses
    and names what it found. It locks both tables first, so nothing can add
    either between its counts and its drops. Without them every approval row
    stays.
    @spec REQUEST-APPROVAL-4.
    """
    bind = op.get_bind()
    # Held to the end of this migration's transaction, so nothing can create a
    # dynamic approval or a registration between the counts and the drops.
    op.execute(f"LOCK TABLE {SCHEMA}.agents, {SCHEMA}.approvals IN ACCESS EXCLUSIVE MODE")
    dynamic = bind.execute(
        sa.text(f"SELECT count(*) FROM {SCHEMA}.approvals WHERE selection_mode = 'dynamic'")
    ).scalar_one()
    registered = bind.execute(
        sa.text(f"SELECT count(*) FROM {SCHEMA}.agents WHERE {_HAS_SELECTOR}")  # noqa: S608
    ).scalar_one()
    if dynamic or registered:
        raise RuntimeError(
            f"refusing to downgrade below revision 0153: {dynamic} dynamic approval(s) and "
            f"{registered} agent(s) with an approver_ticket registration exist. The "
            "downgrade would drop each dynamic approval's selection snapshot and spent "
            "ticket nonce, so the row would read back ordinary and its ticket could be "
            "used again, and no older release enforces a registration. Roll the "
            "application forward instead."
        )
    op.execute(f"DROP TRIGGER approvals_dynamic_unapproved ON {SCHEMA}.approvals")
    op.execute(f"DROP FUNCTION {SCHEMA}.enforce_dynamic_approval_unapproved()")
    op.execute(f"DROP TRIGGER approvals_approver_ticket_route_lock ON {SCHEMA}.approvals")
    op.execute(f"DROP FUNCTION {SCHEMA}.enforce_approver_ticket_route_lock()")
    op.drop_constraint(
        "agents_approver_ticket_requires_requester_identity_ck",
        "agents",
        schema=SCHEMA,
        type_="check",
    )
    op.drop_index("approvals_selection_nonce_key", table_name="approvals", schema=SCHEMA)
    op.drop_constraint("approvals_selection_snapshot_ck", "approvals", schema=SCHEMA, type_="check")
    op.drop_constraint("approvals_selection_mode_ck", "approvals", schema=SCHEMA, type_="check")
    for column in reversed(("selection_mode", *_SNAPSHOT_COLUMNS)):
        op.drop_column("approvals", column, schema=SCHEMA)

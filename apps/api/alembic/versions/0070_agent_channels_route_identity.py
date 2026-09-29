"""a Slack route names its identity, and a route is keyed by the triple (ADR-0168 decision 3)

- `agent_channels.adapter` is backfilled to `'default'` for every Slack binding,
  and so is `reply_adapter` on Slack approvals (ADR-0168 decision 5) and
  publications, whatever their `reply_endpoint`: on those rows it is the CLI
  stub's per-turn Slack Web API base, not a transport.
- 0024's both-or-neither `agent_channels_route_pair_ck` becomes the kind-aware
  `agent_channels_route_ck`: a Slack route has an adapter and no endpoint, and
  any other kind is unchanged.
- 0023's `(kind, address)` key becomes `agent_channels_route_key`, UNIQUE NULLS
  NOT DISTINCT on `(kind, address, adapter)`. The pair leads so a lookup on
  `(kind, address)` keeps the index prefix, and NULLS NOT DISTINCT keeps two
  route-less non-Slack rows colliding on the pair, as under 0023.

**Contract** (`revision_kinds.json`), as 0041 was: an application that stores
`'default'` cannot serve a database whose 0024 check refuses it, so the window
floor moves to this revision and an upgrade across it needs `--forward-only`.
It ships in v0.11.0 with the readers that compare a Slack route's identity
through `aci_protocol.turn.route_identity`, so every upgrade from v0.10.x
crosses it, and a rollback below v0.11.0 is refused as for any contract.
v0.10.x pods still serving during the roll keep running against it: their
schema check treats a revision they do not know as a compatible expand, runs
only at API startup, and the worker has none. Those API pods write a Slack
binding with no identity, which the new check refuses; compare a publication
replay's `reply_adapter` raw, so a replay of a publication stored before this
revision, now `'default'`, is refused as a conflict; and refuse a publication
create whose Slack reply names `'default'` with no endpoint, the shape every
Slack turn carries from here. The chart's worker upgrade drain holds worker
claims from before this revision until the roll has finished, which keeps the
last failure, and v0.10.x workers resolving by pair against the triple key,
out of the window unless the drain is disabled or ends early.

The pre-flight refuses, before anything moves, while a Slack binding or a Slack
approval notification still carries an endpoint, the pre-ADR custom-transport
form retired here, or while an approval raised through one is pending or owed
its resume, since the resume replays that transport's slug. It names each row
by agent, address and adapter slug, never by endpoint value, which can carry a
token (0024's `_redacted` rule).

`downgrade` refuses while a Slack identity other than `'default'` is bound, or
while two rows share one `(kind, address)`: 0023's key cannot hold either. It
then restores 0023's key and 0024's check, and hands NULL back to every Slack
route and to every Slack reply stored as `'default'`, because the pre-0070
application compares `reply_adapter` raw on a replay. A reply raised under a
named identity keeps its name. It never writes `generation`, which is what
revokes a token minted before a rebind.

Revision ID: 0070
Revises: 0069
Create Date: 2026-09-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from curie_api.migration_fence import fence_identity_tables

revision: str = "0070"
down_revision: str | None = "0069"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "curie"
TABLE = "agent_channels"
SLACK = "slack"
DEFAULT_IDENTITY = "default"
REPLY_TABLES = ("approvals", "publications")

# Named explicitly, following 0021's discipline, so the API can translate them
# and the tests can look them up by identity rather than by shape.
OLD_KEY = "agent_channels_kind_address_key"
OLD_CHECK = "agent_channels_route_pair_ck"
OLD_RULE = "(endpoint IS NULL) = (adapter IS NULL)"
ROUTE_KEY = "agent_channels_route_key"
ROUTE_CHECK = "agent_channels_route_ck"
ROUTE_RULE = (
    "(kind = 'slack' AND adapter IS NOT NULL AND endpoint IS NULL) "
    "OR (kind <> 'slack' AND (endpoint IS NULL) = (adapter IS NULL))"
)


def _custom_transport(conn: sa.engine.Connection) -> list[str]:
    """Every Slack route still on the custom-transport form, named without its endpoint."""

    bindings = conn.execute(
        sa.text(
            f"SELECT a.name, c.agent_id, c.address, c.adapter "
            f"FROM {SCHEMA}.{TABLE} c JOIN {SCHEMA}.agents a ON a.id = c.agent_id "
            "WHERE c.kind = :slack AND c.endpoint IS NOT NULL ORDER BY a.name, c.address"
        ),
        {"slack": SLACK},
    ).all()
    # LATERAL over a guarded value: jsonb_each raises on a non-object, and a
    # WHERE clause does not order its evaluation ahead of the function.
    notifications = conn.execute(
        sa.text(
            "SELECT a.name, a.id, r.key, r.value -> 'notification' ->> 'address', "
            "r.value -> 'notification' ->> 'adapter' "
            f"FROM {SCHEMA}.agents a CROSS JOIN LATERAL jsonb_each("
            "CASE WHEN jsonb_typeof(a.approval_routes) = 'object' "
            "THEN a.approval_routes ELSE '{}'::jsonb END) AS r "
            "WHERE jsonb_typeof(r.value) = 'object' "
            "AND r.value -> 'notification' ->> 'kind' = :slack "
            "AND r.value -> 'notification' ->> 'endpoint' IS NOT NULL "
            "ORDER BY a.name, r.key"
        ),
        {"slack": SLACK},
    ).all()
    # An approval whose resume is still owed replays its reply route verbatim
    # (`resumequeue._build_turn`), so the transport's slug would name no
    # identity once the binding is rebound, and the resume turn is dropped.
    # Rewriting it to 'default' would instead send the resume through a
    # different transport than the card went out on; naming it lets it be
    # settled on the running release, which loses nothing. The resumable
    # statuses are `crud._RESUMABLE_STATUSES`; a publication's approval is never
    # resumed (`crud.list_resolved_unresumed`).
    approvals = conn.execute(
        sa.text(
            "SELECT ap.id, a.name, ap.agent_id, ap.reply_channel, ap.reply_adapter, ap.status "
            f"FROM {SCHEMA}.approvals ap LEFT JOIN {SCHEMA}.agents a ON a.id = ap.agent_id "
            "WHERE ap.reply_kind = :slack AND ap.reply_endpoint IS NOT NULL "
            "AND ap.reply_adapter IS NOT NULL "
            "AND (ap.status = 'pending' OR (ap.purpose <> 'publication' "
            "AND ap.status IN ('approved', 'rejected', 'expired') "
            "AND ap.resolved_at IS NOT NULL AND ap.resumed_at IS NULL)) "
            "ORDER BY a.name, ap.id"
        ),
        {"slack": SLACK},
    ).all()
    return (
        [
            f"agent {name!r} ({agent_id}) binds slack:{address} with adapter {adapter!r}"
            for name, agent_id, address, adapter in bindings
        ]
        + [
            f"agent {name!r} ({agent_id}) approval route {route!r} notifies slack:{address} "
            f"through an endpoint with adapter {adapter!r}"
            for name, agent_id, route, address, adapter in notifications
        ]
        + [
            f"approval {approval_id} of agent {name!r} ({agent_id}) replies to "
            f"slack:{address} through an endpoint with adapter {adapter!r} and "
            + ("is pending" if approval_status == "pending" else "is owed its resume")
            for approval_id, name, agent_id, address, adapter, approval_status in approvals
        ]
    )


def upgrade() -> None:
    conn = op.get_bind()

    # The identity fence, the same two tables in the same order and mode as
    # 0022 and 0024, so none of them can deadlock another; see `migration_fence`.
    fence_identity_tables(conn)

    refused = _custom_transport(conn)
    if refused:
        raise RuntimeError(
            "cannot upgrade to 0070: these Slack routes still carry an endpoint, the "
            "custom-transport form ADR-0168 decision 3 retires -- "
            + "; ".join(refused)
            + ". Settle each approval listed first (resolve it, or let it expire) and "
            "let its resume turn finish, because a resume replays the transport it was "
            "raised through. Then clear the route on each binding (PATCH "
            "/agents/<agent id>/channels?kind=slack&address=<address> with the body "
            '{"kind": "slack", "address": "<address>", "endpoint": null, "adapter": null}'
            "), which this upgrade binds as the default identity, or delete it, and drop "
            "endpoint and adapter from each notification, then re-run the upgrade. A "
            "named identity can be bound once the upgrade has finished. Nothing was "
            "changed."
        )

    # The old check goes first: a Slack row naming 'default' with no endpoint is
    # exactly the shape `(endpoint IS NULL) = (adapter IS NULL)` refuses.
    op.drop_constraint(OLD_CHECK, TABLE, type_="check", schema=SCHEMA)
    conn.execute(
        sa.text(
            f"UPDATE {SCHEMA}.{TABLE} SET adapter = :identity "
            "WHERE kind = :slack AND adapter IS NULL"
        ),
        {"identity": DEFAULT_IDENTITY, "slack": SLACK},
    )
    op.create_check_constraint(ROUTE_CHECK, TABLE, ROUTE_RULE, schema=SCHEMA)
    op.drop_constraint(OLD_KEY, TABLE, type_="unique", schema=SCHEMA)
    op.create_unique_constraint(
        ROUTE_KEY,
        TABLE,
        ["kind", "address", "adapter"],
        schema=SCHEMA,
        postgresql_nulls_not_distinct=True,
    )
    for table in REPLY_TABLES:
        conn.execute(
            sa.text(
                f"UPDATE {SCHEMA}.{table} SET reply_adapter = :identity "
                "WHERE reply_kind = :slack AND reply_adapter IS NULL"
            ),
            {"identity": DEFAULT_IDENTITY, "slack": SLACK},
        )


def downgrade() -> None:
    conn = op.get_bind()
    fence_identity_tables(conn)

    named = conn.execute(
        sa.text(
            f"SELECT a.name, c.agent_id, c.address, c.adapter "
            f"FROM {SCHEMA}.{TABLE} c JOIN {SCHEMA}.agents a ON a.id = c.agent_id "
            "WHERE c.kind = :slack AND c.adapter <> :identity ORDER BY a.name, c.address"
        ),
        {"slack": SLACK, "identity": DEFAULT_IDENTITY},
    ).all()
    if named:
        rows = "; ".join(
            f"agent {name!r} ({agent_id}) on slack:{address} as {identity!r}"
            for name, agent_id, address, identity in named
        )
        raise RuntimeError(
            "cannot downgrade below 0070: a Slack identity other than 'default' is bound "
            f"({rows}). The pre-0070 schema has one Slack app; delete those bindings "
            "first. Nothing was changed."
        )
    shared = conn.execute(
        sa.text(
            "SELECT kind, address, string_agg(agent_id::text "
            "|| COALESCE(' as ' || adapter, ''), ', ' ORDER BY agent_id::text) "
            f"FROM {SCHEMA}.{TABLE} GROUP BY kind, address HAVING count(*) > 1 "
            "ORDER BY kind, address"
        )
    ).all()
    if shared:
        rows = "; ".join(f"{kind}:{address} held by {held}" for kind, address, held in shared)
        raise RuntimeError(
            "cannot downgrade below 0070: several routes share one (kind, address), which "
            f"the pre-0070 key holds to one row ({rows}). Delete all but one of each "
            "first. Nothing was changed."
        )

    op.drop_constraint(ROUTE_KEY, TABLE, type_="unique", schema=SCHEMA)
    op.create_unique_constraint(OLD_KEY, TABLE, ["kind", "address"], schema=SCHEMA)
    op.drop_constraint(ROUTE_CHECK, TABLE, type_="check", schema=SCHEMA)
    conn.execute(
        sa.text(f"UPDATE {SCHEMA}.{TABLE} SET adapter = NULL WHERE kind = :slack"),
        {"slack": SLACK},
    )
    op.create_check_constraint(OLD_CHECK, TABLE, OLD_RULE, schema=SCHEMA)
    for table in REPLY_TABLES:
        conn.execute(
            sa.text(
                f"UPDATE {SCHEMA}.{table} SET reply_adapter = NULL "
                "WHERE reply_kind = :slack AND reply_adapter = :identity"
            ),
            {"slack": SLACK, "identity": DEFAULT_IDENTITY},
        )

"""Migration 0024 gives a binding its server-controlled route and its generation.

`0024_agent_channels_endpoint_adapter_generation.py` adds `endpoint` (nullable),
`adapter` (nullable), `generation` (NOT NULL, default 0), and the CHECK
constraint `agent_channels_route_pair_ck` enforcing `(endpoint IS NULL) =
(adapter IS NULL)`.

Why the CHECK exists at all, given `ChannelBinding` already validates the pair
(plan EB-A18): the two columns are independently nullable, so a HALF-configured
route is representable, and a row written out of band -- a restored dump, an
operator's psql, a future code path -- would pass ingress and then fail inside
the worker, far from its cause. The DB states the invariant true for every kind;
the schema layer states the kind-specific requirement (a non-Slack binding needs
BOTH). Two layers, one for representability and one for policy, rather than a
CHECK that has to know what `slack` means.

Why the downgrade refuses: dropping these columns destroys the only record of
where a live adapter's traffic goes and which credential authenticates it. A
re-upgrade cannot reconstruct either (0024's backfill is all-NULL by
construction), and a worker running against a downgraded schema fails closed on
every non-Slack turn.

Follows `test_migration_0021_agent_channels.py`: a throwaway database all to
itself via `isolated_migration_db`, real Postgres, no mocking.
"""

from __future__ import annotations

import uuid

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    constraint_exists,
    sql_rows,
)
from alembic import command
from alembic.config import Config

# Targeted explicitly, never as a relative "-1" (#1391).
BELOW = "0023"
REVISION = "0024"

ROUTE_PAIR_CHECK = "agent_channels_route_pair_ck"
ROUTE_COLUMNS = ("endpoint", "adapter", "generation")


def _at_below(db: IsolatedMigrationDb) -> Config:
    db.at(BELOW)
    return alembic_config()


def _seed_agent(name: str) -> uuid.UUID:
    agent_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, :name)",
        {"id": agent_id, "name": name},
    )
    return agent_id


def _insert_binding(
    name: str,
    kind: str,
    address: str,
    *,
    endpoint: str | None = None,
    adapter: str | None = None,
) -> uuid.UUID:
    """A raw `agent_channels` INSERT -- deliberately NOT through the API.

    Out-of-band writes are the whole reason the database-level invariant exists
    (T-A16): the application validator cannot see them.
    """

    agent_id = _seed_agent(name)
    sql_rows(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address, endpoint, adapter) "
        "VALUES (:id, :agent, :kind, :addr, :endpoint, :adapter)",
        {
            "id": uuid.uuid4(),
            "agent": agent_id,
            "kind": kind,
            "addr": address,
            "endpoint": endpoint,
            "adapter": adapter,
        },
    )
    return agent_id


def _seed_slack_binding(name: str, address: str) -> uuid.UUID:
    agent_id = _seed_agent(name)
    sql_rows(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
        "VALUES (:id, :agent, 'slack', :addr)",
        {"id": uuid.uuid4(), "agent": agent_id, "addr": address},
    )
    return agent_id


def test_the_upgrade_adds_the_route_columns_with_a_null_backfill(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """AC13c, upgrade half. The backfill is trivially all-NULL/zero (no binding
    has a route today), which is exactly why cutover steps 8-11 exist: every
    non-Slack binding comes out of this migration UNROUTABLE and must be
    reconfigured before ingress restarts.

    `generation` is NOT NULL at 0 rather than nullable: a NULL generation cannot
    be compared against a token's claim, so a nullable column would make D5's
    rebind check silently vacuous for pre-0024 rows.
    """

    cfg = _at_below(isolated_migration_db)
    _seed_slack_binding("slack-agent", "C0EXAMPLE1")

    command.upgrade(cfg, REVISION)

    assert set(ROUTE_COLUMNS) <= column_names("agent_channels")
    rows = sql_rows(
        "SELECT endpoint, adapter, generation FROM curie.agent_channels "
        "WHERE address = 'C0EXAMPLE1'"
    )
    assert rows == [(None, None, 0)]

    nullable = sql_rows(
        "SELECT column_name, is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = 'agent_channels' "
        "AND column_name = ANY(:cols)",
        {"cols": list(ROUTE_COLUMNS)},
    )
    assert dict(nullable) == {"endpoint": "YES", "adapter": "YES", "generation": "NO"}


@pytest.mark.parametrize(
    ("endpoint", "adapter"),
    [
        ("http://curie-mail-adapter:8080/", None),
        (None, "agentmail-sandbox"),
    ],
)
def test_a_half_configured_route_is_rejected_by_the_database(
    isolated_migration_db: IsolatedMigrationDb, endpoint: str | None, adapter: str | None
) -> None:
    """T-A16 / AC13b. Both directions of the pair, because a CHECK written as
    `endpoint IS NOT NULL OR adapter IS NULL` catches one and waves the other
    through.

    The reviewer's failure mode was a row that "accepts ingress and fails later
    in the worker" -- a route half-written by hand or by a restored dump. Only a
    database-level invariant forecloses it for rows the application never sees,
    which is why T-C11/T-C12 (the schema half) are not sufficient on their own.
    """

    cfg = _at_below(isolated_migration_db)
    command.upgrade(cfg, REVISION)

    with pytest.raises(Exception) as caught:
        _insert_binding(
            "half-routed", "email", "ops@example.test", endpoint=endpoint, adapter=adapter
        )
    assert ROUTE_PAIR_CHECK in str(caught.value), caught.value


def test_both_set_and_both_absent_are_accepted(isolated_migration_db: IsolatedMigrationDb) -> None:
    """T-A16's positive control. Without it, a CHECK of `false` would pass the
    two rejection cases above and make every binding uninsertable.
    """

    cfg = _at_below(isolated_migration_db)
    command.upgrade(cfg, REVISION)

    _insert_binding(
        "fully-routed",
        "email",
        "ops@example.test",
        endpoint="http://curie-mail-adapter:8080/",
        adapter="agentmail-sandbox",
    )
    # Slack's route is legitimately implicit: the worker's configured origin.
    _insert_binding("slack-agent", "slack", "C0EXAMPLE1")

    assert len(sql_rows("SELECT 1 FROM curie.agent_channels")) == 2
    assert constraint_exists(ROUTE_PAIR_CHECK)


def _seed_approval(*, reply_channel: str, reply_kind: str, status: str) -> uuid.UUID:
    """One approval on the 0023 schema (`reply_kind` exists, `reply_adapter` is
    NULL for every row until this revision fills it)."""

    approval_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.approvals "
        "(id, conversation_id, author, summary, reply_kind, reply_channel, "
        " reply_placeholder, dedupe_key, status) "
        "VALUES (:id, :conv, 'U1', 'send the quote', :kind, :chan, 'p-1', :dedupe, :status)",
        {
            "id": approval_id,
            "conv": f"th-{approval_id.hex[:8]}",
            "kind": reply_kind,
            "chan": reply_channel,
            "dedupe": approval_id.hex,
            "status": status,
        },
    )
    return approval_id


def _reply_adapter(approval_id: uuid.UUID) -> str | None:
    rows = sql_rows(
        "SELECT reply_adapter FROM curie.approvals WHERE id = :id", {"id": approval_id}
    )
    assert rows, f"approval {approval_id} vanished"
    return rows[0][0]


@pytest.mark.parametrize("status", ["pending", "approved", "rejected", "expired"])
def test_the_upgrade_refuses_a_non_slack_approval_with_no_adapter_provenance(
    isolated_migration_db: IsolatedMigrationDb, status: str
) -> None:
    """0022's other half, landed here because `adapter` does not exist until this
    revision. A non-Slack approval whose binding names no egress identity has no
    adapter provenance ANYWHERE in the schema, and `resumequeue` rebuilds the
    resume turn from the approval row -- so resuming it POSTs with no credential
    and fails closed inside the worker, days later and far from any request.

    Every status is swept, not just `pending`: all of approved/rejected/expired
    are in `crud._RESUMABLE_STATUSES` (`crud.py:622-626`) and
    `crud.reopen_dead_lettered_resume` can re-owe a wake on an already-resumed
    row (#532), so no status is inert here either.

    Mutations this catches: delete the refusal predicate and the migration
    completes leaving `reply_adapter` NULL on a row that needs it; narrow the
    predicate to `pending` and three of the four cases go green while the deferred
    misroute stands. The message must name the id, the channel, the kind AND the
    status, because the operator's fix differs by status (resolve it, or configure
    the binding's route).
    """

    cfg = _at_below(isolated_migration_db)
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, 'mail-agent')",
        {"id": (agent_id := uuid.uuid4())},
    )
    sql_rows(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
        "VALUES (:id, :agent, 'email', 'agent@example.test')",
        {"id": uuid.uuid4(), "agent": agent_id},
    )
    approval = _seed_approval(
        reply_channel="agent@example.test", reply_kind="email", status=status
    )

    with pytest.raises(Exception) as caught:
        command.upgrade(cfg, REVISION)

    message = str(caught.value)
    assert str(approval) in message, message
    assert "agent@example.test" in message, message
    assert "email" in message, message
    assert status in message, message


@pytest.mark.parametrize("status", ["pending", "approved", "rejected", "expired"])
def test_a_slack_approval_upgrades_with_a_null_reply_adapter(
    isolated_migration_db: IsolatedMigrationDb, status: str
) -> None:
    """The sibling lane, and the positive control for the refusal above.

    Slack's reply route is the worker's configured origin (D4.4), so a Slack
    approval with no adapter is COMPLETE, not unroutable. NULL is the honest
    value and the migration must leave it alone.

    Mutations this catches: widen the refusal predicate to every approval and the
    cutover cannot run at all on a Slack install; or "fix" the NULL by stamping a
    placeholder slug, which would make the worker look up a credential for an
    adapter that does not own the turn.
    """

    cfg = _at_below(isolated_migration_db)
    sql_rows(
        "INSERT INTO curie.agents (id, name) VALUES (:id, 'slack-agent')",
        {"id": (agent_id := uuid.uuid4())},
    )
    sql_rows(
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address) "
        "VALUES (:id, :agent, 'slack', 'C0EXAMPLE1')",
        {"id": uuid.uuid4(), "agent": agent_id},
    )
    approval = _seed_approval(
        reply_channel="C0EXAMPLE1", reply_kind="slack", status=status
    )

    command.upgrade(cfg, REVISION)

    assert _reply_adapter(approval) is None
    assert sql_rows(
        "SELECT reply_kind FROM curie.approvals WHERE id = :id", {"id": approval}
    ) == [("slack",)]


def test_the_round_trip_succeeds_on_a_slack_only_database(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A14, round-trip half / AC13c.

    upgrade -> downgrade -> upgrade with no binding carrying a route. The
    ordering trap this catches: the CHECK constraint references `endpoint` and
    `adapter`, so a downgrade that drops the columns before the constraint fails
    outright (or, worse, leaves a constraint behind that blocks the re-upgrade's
    own ADD CONSTRAINT). Reverse creation order is the fix, and the second
    upgrade is what proves it.
    """

    cfg = _at_below(isolated_migration_db)
    _seed_slack_binding("slack-agent", "C0EXAMPLE1")

    command.upgrade(cfg, REVISION)
    command.downgrade(cfg, BELOW)

    assert not constraint_exists(ROUTE_PAIR_CHECK)
    assert column_names("agent_channels").isdisjoint(ROUTE_COLUMNS)
    # The binding itself is untouched: a downgrade drops the route, never the row.
    assert len(sql_rows("SELECT 1 FROM curie.agent_channels")) == 1

    command.upgrade(cfg, REVISION)
    assert constraint_exists(ROUTE_PAIR_CHECK)
    assert set(ROUTE_COLUMNS) <= column_names("agent_channels")


def test_the_downgrade_refuses_and_names_any_routed_binding(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A14, refusal half / AC13c.

    Mutation this catches: make `downgrade` an unconditional `drop_column`. The
    round-trip test above still passes (it has no routed binding), and the only
    signal is a production database that has silently forgotten where its email
    adapter lives and which credential authenticates the platform to it.
    """

    cfg = _at_below(isolated_migration_db)
    command.upgrade(cfg, REVISION)
    _insert_binding(
        "mail-agent",
        "email",
        "ops@example.test",
        endpoint="http://curie-mail-adapter:8080/",
        adapter="agentmail-sandbox",
    )
    _seed_slack_binding("slack-agent", "C0EXAMPLE1")

    with pytest.raises(Exception) as caught:
        command.downgrade(cfg, BELOW)

    message = str(caught.value)
    assert "ops@example.test" in message, message
    assert "agentmail-sandbox" in message or "curie-mail-adapter" in message, message
    # The unrouted slack binding is not blamed for its neighbour's state.
    assert "C0EXAMPLE1" not in message, message
    # And the refusal was total: the route survives.
    assert sql_rows(
        "SELECT adapter FROM curie.agent_channels WHERE address = 'ops@example.test'"
    ) == [("agentmail-sandbox",)]


def test_the_downgrade_refuses_a_rebound_binding_whose_generation_is_nonzero(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """T-A14, the revocation half.

    An unrouted Slack binding passes the route predicate, and its generation is
    still a durable SECURITY fact: a token claim binds `(channel_id, generation)`
    and `update_agent_binding` mutates the row in place, so the id survives every
    rebind. Drop the column and re-upgrade (which restores it at 0) and every
    credential those rebinds revoked is authoritative again for the same binding
    id -- revocation silently undone.

    Mutation this catches: delete the generation predicate. Every other test in
    this file still passes (they all sit at generation 0), and the only signal is
    a revoked adapter token that starts working again.
    """

    cfg = _at_below(isolated_migration_db)
    command.upgrade(cfg, REVISION)
    _seed_slack_binding("rebound-agent", "C0EXAMPLE1")
    _seed_slack_binding("settled-agent", "C0EXAMPLE2")
    # Two rebinds, exactly as `update_agent_binding` would have counted them.
    sql_rows(
        "UPDATE curie.agent_channels SET generation = 2 WHERE address = 'C0EXAMPLE1'"
    )

    with pytest.raises(Exception) as caught:
        command.downgrade(cfg, BELOW)

    message = str(caught.value)
    assert "C0EXAMPLE1" in message, message
    assert "generation 2" in message, message
    # The never-rebound binding is not blamed for its neighbour's state.
    assert "C0EXAMPLE2" not in message, message
    # And the refusal was total: the generation survives to be rotated against.
    assert sql_rows(
        "SELECT generation FROM curie.agent_channels WHERE address = 'C0EXAMPLE1'"
    ) == [(2,)]

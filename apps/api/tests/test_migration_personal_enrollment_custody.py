"""Migration 0160 adds personal Claude custody and relaxes terminal shapes (#5377 AC3).

An expand revision above 0159. It adds ``curie.personal_claude_custody`` (a
token-free reference to one enrollment generation's grant Secret) and relaxes
``execution_requests_state_shape_ck`` and
``execution_requests_teardown_unconfirmed_ck`` so an enrollment fence can stop
a running request (``enrollment_fenced``) and confirm the teardown of a started
terminal one. Rows are seeded with raw SQL at the revision they belong to, never
through the current models. Every refusal probe is paired with the insert the
same schema accepts, and each relaxed row is first shown refused at 0159, so a
revert of the 0160 CHECK text turns the test red.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    sql_dicts,
    sql_rows,
    stamped_revision,
)
from alembic import command
from sqlalchemy.exc import IntegrityError

REVISION = "0160"
BELOW = "0159"
CUSTODY = "personal_claude_custody"
KEY = "CLAUDE_CODE_OAUTH_TOKEN"
NAMESPACE = "acme-personal-grants"
STARTED = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
TERMINAL = STARTED + timedelta(minutes=20)
WAIT_DEADLINE = STARTED + timedelta(hours=1)
LEASE = STARTED + timedelta(minutes=5)
OBSERVATION = "claims=acme-claim sandboxes=acme-sandbox absent_at=2026-11-10T12:30:00+00:00"
CONSTRAINTS = ("execution_requests_state_shape_ck", "execution_requests_teardown_unconfirmed_ck")
CUSTODY_COLUMNS = {
    "id",
    "tenant_id",
    "principal_id",
    "enrollment_id",
    "generation",
    "secret_namespace",
    "secret_name",
    "secret_key",
    "secret_uid",
    "state",
    "staged_at",
    "lease_expires_at",
    "published_at",
    "retired_at",
    "secret_absent_at",
    "version",
}


def _insert(table: str, values: dict[str, Any]) -> None:
    columns = ", ".join(values)
    params = ", ".join(f":{name}" for name in values)
    sql_rows(f"INSERT INTO curie.{table} ({columns}) VALUES ({params})", values)


def _refused(table: str, values: dict[str, Any]) -> None:
    with pytest.raises(IntegrityError):
        _insert(table, values)


def _tables() -> set[str]:
    return {
        row["table_name"]
        for row in sql_dicts(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'curie'"
        )
    }


def _definitions() -> dict[str, str]:
    return {
        row["conname"]: row["definition"]
        for row in sql_dicts(
            "SELECT conname, pg_get_constraintdef(oid) AS definition FROM pg_constraint "
            "WHERE conrelid = 'curie.execution_requests'::regclass "
            "AND conname = ANY(:names)",
            {"names": list(CONSTRAINTS)},
        )
    }


def _tenant() -> UUID:
    tenant_id = uuid.uuid4()
    _insert(
        "tenants",
        {"id": tenant_id, "deployment_id": f"acme-custody-{tenant_id}", "status": "active"},
    )
    return tenant_id


def _principal(tenant_id: UUID) -> UUID:
    principal_id = uuid.uuid4()
    _insert(
        "principals",
        {
            "id": principal_id,
            "tenant_id": tenant_id,
            "idp_subject": f"acme-custody-{principal_id}",
            "type": "human",
            "status": "active",
        },
    )
    return principal_id


def _work_item(issue: str) -> UUID:
    """A service WorkItem of a fresh agent, so request probes never share one."""

    # A numeric issue id, unique per call.
    issue = f"{issue}{uuid.uuid4().int % 10**9:09d}"
    agent_id = uuid.uuid4()
    _insert("agents", {"id": agent_id, "name": f"acme-bot-{agent_id.hex[:8]}"})
    work_item_id = uuid.uuid4()
    _insert(
        "work_items",
        {
            "id": work_item_id,
            "tracker_kind": "github",
            "tracker_host": "github.com",
            "tracker_scope_id": "101",
            "tracker_issue_id": issue,
            "code_host_kind": "github",
            "code_host_host": "github.com",
            "repository_project_id": "101",
            "code_host_installation_id": 202,
            "agent_id": agent_id,
            "repository_path": "acme-corp/acme-bot",
            "conversation_id": f"acme-custody-{issue}",
            "version": 1,
            "next_sequence": 2,
        },
    )
    return work_item_id


def _started(issue: str, status: str, cause: str | None, **extra: Any) -> dict[str, Any]:
    """A started request on its own WorkItem, terminal unless ``status`` is active."""

    terminal = status not in ("running", "cancellation_requested")
    return {
        "id": uuid.uuid4(),
        "work_item_id": _work_item(issue),
        "sequence": 1,
        "status": status,
        "version": 2,
        "execution_attempts": 1,
        "wait_deadline": WAIT_DEADLINE,
        "started_at": STARTED,
        "execution_deadline": STARTED + timedelta(hours=1),
        "terminal_at": TERMINAL if terminal else None,
        "terminal_cause": cause,
        **extra,
    }


def _never_started(issue: str, status: str, cause: str, **extra: Any) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "work_item_id": _work_item(issue),
        "sequence": 1,
        "status": status,
        "version": 2,
        "execution_attempts": 0,
        "wait_deadline": WAIT_DEADLINE,
        "terminal_at": TERMINAL,
        "terminal_cause": cause,
        **extra,
    }


# Rows only 0160 admits; each is refused at 0159 and accepted after the upgrade.
RELAXED: dict[str, tuple[str, str, dict[str, Any]]] = {
    "completed_with_observation": (
        "completed",
        "completed",
        {"termination_observation": OBSERVATION},
    ),
    "completed_with_teardown_flag": (
        "completed",
        "completed",
        {"teardown_unconfirmed_at": TERMINAL},
    ),
    "failed_with_teardown_flag": ("failed", "model_error", {"teardown_unconfirmed_at": TERMINAL}),
    "fenced_cancellation": ("cancellation_requested", "enrollment_fenced", {}),
    "fenced_failure_with_observation": (
        "failed",
        "enrollment_fenced",
        {"termination_observation": OBSERVATION},
    ),
    "ordinary_failure_with_observation": (
        "failed",
        "deadline_halted",
        {"termination_observation": OBSERVATION},
    ),
}


def _relaxed(name: str) -> dict[str, Any]:
    status, cause, extra = RELAXED[name]
    return _started("8000", status, cause, **extra)


def _custody(
    tenant_id: UUID,
    principal_id: UUID,
    *,
    enrollment_id: UUID | None = None,
    generation: int = 1,
    state: str = "staged",
    **extra: Any,
) -> dict[str, Any]:
    """A custody row whose name, key and namespace satisfy their CHECKs."""

    enrollment = enrollment_id or uuid.uuid4()
    row: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": tenant_id,
        "principal_id": principal_id,
        "enrollment_id": enrollment,
        "generation": generation,
        "secret_namespace": NAMESPACE,
        "secret_name": f"curie-pe-{enrollment.hex}-g{generation}",
        "secret_key": KEY,
        "state": state,
        "lease_expires_at": LEASE,
    }
    if state in ("published", "retiring", "retired"):
        row |= {"secret_uid": f"uid-{uuid.uuid4()}", "published_at": STARTED}
    if state == "retired":
        row |= {"retired_at": TERMINAL, "secret_absent_at": TERMINAL}
    return row | extra


def test_0160_upgrade_adds_custody_and_relaxes_terminal_shapes(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    assert CUSTODY not in _tables()
    # Historical rows 0159 already admits keep every value across the upgrade.
    historical = {
        "completed": _started("8100", "completed", "completed"),
        "owner_lost": _started("8101", "failed", "owner_lost", termination_observation=OBSERVATION),
        "settled": _started(
            "8102",
            "cancelled",
            "issue_cancelled",
            termination_observation=OBSERVATION,
            teardown_unconfirmed_at=TERMINAL,
        ),
    }
    for row in historical.values():
        _insert("execution_requests", row)
    before = {
        name: sql_dicts("SELECT * FROM curie.execution_requests WHERE id = :id", {"id": row["id"]})
        for name, row in historical.items()
    }
    relaxed = [_relaxed(name) for name in RELAXED]
    for row in relaxed:
        with pytest.raises(IntegrityError):
            _insert("execution_requests", row)

    command.upgrade(config, REVISION)

    assert stamped_revision() == REVISION
    assert column_names(CUSTODY) == CUSTODY_COLUMNS
    for name, row in historical.items():
        assert (
            sql_dicts("SELECT * FROM curie.execution_requests WHERE id = :id", {"id": row["id"]})
            == before[name]
        )
    for row in relaxed:
        _insert("execution_requests", row)
    _probe_request_shapes()
    _probe_custody()


def _probe_request_shapes() -> None:
    # enrollment_fenced, like owner_lost, needs an observation once failed.
    _refused("execution_requests", _started("8200", "failed", "enrollment_fenced"))
    _refused("execution_requests", _started("8201", "failed", "owner_lost"))
    _insert("execution_requests", _started("8202", "failed", "model_error"))
    # A cause outside the stopping set still cannot request cancellation.
    _refused(
        "execution_requests", _started("8203", "cancellation_requested", "authentication_required")
    )
    _insert(
        "execution_requests",
        _started("8204", "failed", "authentication_required", termination_observation=OBSERVATION),
    )
    # The teardown flag needs a started completed, failed or expired request.
    _refused(
        "execution_requests",
        _never_started("8205", "failed", "start_failed", teardown_unconfirmed_at=TERMINAL),
    )
    _refused(
        "execution_requests",
        _never_started(
            "8206", "expired", "capacity_wait_expired", teardown_unconfirmed_at=TERMINAL
        ),
    )
    _insert(
        "execution_requests",
        _started(
            "8207",
            "expired",
            "execution_deadline",
            termination_observation=OBSERVATION,
            teardown_unconfirmed_at=TERMINAL,
        ),
    )
    _refused(
        "execution_requests",
        _started("8208", "running", None, teardown_unconfirmed_at=TERMINAL),
    )
    _insert("execution_requests", _started("8209", "running", None))
    # A blank observation stays refused on every relaxed branch.
    _refused(
        "execution_requests",
        _started("8210", "completed", "completed", termination_observation="  "),
    )


def _probe_custody() -> None:
    tenant = _tenant()
    other_tenant = _tenant()
    owner = _principal(tenant)
    peer = _principal(tenant)
    foreign = _principal(other_tenant)

    # The key, name and namespace are pinned to the shared custody contract.
    _refused(CUSTODY, _custody(tenant, owner, secret_key="ANTHROPIC_API_KEY"))
    _refused(CUSTODY, _custody(tenant, owner, secret_namespace="Acme_Grants"))
    _refused(CUSTODY, _custody(tenant, owner, secret_namespace="a" * 64))
    enrollment = uuid.uuid4()
    _refused(
        CUSTODY,
        _custody(
            tenant, owner, enrollment_id=enrollment, secret_name=f"curie-pe-{enrollment.hex}-g2"
        ),
    )
    _refused(
        CUSTODY,
        _custody(
            tenant, owner, enrollment_id=enrollment, secret_name=f"curie-pe-{uuid.uuid4().hex}-g1"
        ),
    )
    _refused(
        CUSTODY,
        _custody(tenant, owner, enrollment_id=enrollment, secret_name=f"curie-pe-{enrollment}-g1"),
    )
    _refused(CUSTODY, _custody(tenant, owner, generation=0))
    _refused(CUSTODY, _custody(tenant, owner, state="pending"))
    _refused(CUSTODY, _custody(tenant, owner, version=0))
    # A principal of another tenant; liveness is the owner's own row below.
    _refused(CUSTODY, _custody(tenant, foreign, enrollment_id=enrollment))
    staged = _custody(tenant, owner, enrollment_id=enrollment)
    _insert(CUSTODY, staged)
    assert sql_dicts(
        "SELECT version, secret_uid, published_at, staged_at IS NOT NULL AS stamped "
        "FROM curie.personal_claude_custody WHERE id = :id",
        {"id": staged["id"]},
    ) == [{"version": 1, "secret_uid": None, "published_at": None, "stamped": True}]

    # One staged row per owner, whatever its enrollment id.
    _refused(CUSTODY, _custody(tenant, owner, generation=2))
    _insert(CUSTODY, _custody(tenant, peer))
    # One live row per (enrollment, generation); an abandoned one may repeat it.
    _refused(CUSTODY, _custody(tenant, owner, enrollment_id=enrollment, state="published"))
    _insert(
        CUSTODY,
        _custody(tenant, owner, enrollment_id=enrollment, state="abandoned", secret_absent_at=None),
    )
    _insert(
        CUSTODY,
        _custody(
            tenant, owner, enrollment_id=enrollment, state="abandoned", secret_absent_at=TERMINAL
        ),
    )

    # One published row per enrollment.
    published = uuid.uuid4()
    _insert(CUSTODY, _custody(tenant, owner, enrollment_id=published, state="published"))
    _refused(
        CUSTODY, _custody(tenant, owner, enrollment_id=published, generation=2, state="published")
    )
    _insert(
        CUSTODY, _custody(tenant, owner, enrollment_id=published, generation=2, state="retiring")
    )
    _insert(
        CUSTODY, _custody(tenant, owner, enrollment_id=published, generation=3, state="retired")
    )

    # Shape: a published reference needs its UID and publish time; staged and
    # abandoned rows were never published; only abandoned or retired rows
    # record a verified absence, and a retired row always does.
    shape = uuid.uuid4()
    _refused(
        CUSTODY, _custody(tenant, peer, enrollment_id=shape, state="published", secret_uid=None)
    )
    _refused(
        CUSTODY, _custody(tenant, peer, enrollment_id=shape, state="published", secret_uid=" ")
    )
    _refused(
        CUSTODY, _custody(tenant, peer, enrollment_id=shape, state="published", published_at=None)
    )
    _refused(
        CUSTODY,
        _custody(tenant, peer, enrollment_id=shape, state="abandoned", published_at=STARTED),
    )
    _refused(
        CUSTODY,
        _custody(tenant, peer, enrollment_id=shape, state="retiring", retired_at=TERMINAL),
    )
    _refused(
        CUSTODY,
        _custody(tenant, peer, enrollment_id=shape, state="retired", secret_absent_at=None),
    )
    _refused(CUSTODY, _custody(tenant, peer, enrollment_id=shape, state="retired", retired_at=None))
    _refused(
        CUSTODY,
        _custody(tenant, peer, enrollment_id=shape, state="retiring", secret_absent_at=TERMINAL),
    )
    _insert(CUSTODY, _custody(tenant, peer, enrollment_id=shape, state="published"))


def _seed_downgrade_blocker(blocker: str) -> None:
    if blocker == "custody_row":
        tenant = _tenant()
        _insert(CUSTODY, _custody(tenant, _principal(tenant)))
        return
    _insert("execution_requests", _relaxed(blocker))


def _clear_downgrade_blockers() -> None:
    sql_rows(f"DELETE FROM curie.{CUSTODY}")
    sql_rows(
        "DELETE FROM curie.execution_requests WHERE terminal_cause = 'enrollment_fenced' "
        "OR (status = 'completed' AND termination_observation IS NOT NULL) "
        "OR (status = 'failed' AND started_at IS NOT NULL AND terminal_cause <> 'owner_lost' "
        "AND termination_observation IS NOT NULL) "
        "OR (status <> 'cancelled' AND teardown_unconfirmed_at IS NOT NULL)"
    )


@pytest.mark.parametrize(
    "blocker",
    [
        "custody_row",
        "fenced_cancellation",
        "fenced_failure_with_observation",
        "completed_with_observation",
        "ordinary_failure_with_observation",
        "completed_with_teardown_flag",
        "failed_with_teardown_flag",
    ],
)
def test_0160_downgrade_refuses_relaxed_rows_then_restores_0159_exactly(
    isolated_migration_db: IsolatedMigrationDb, blocker: str
) -> None:
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    prior = _definitions()
    assert set(prior) == set(CONSTRAINTS)
    # Rows 0159 admits never hold the downgrade: an owner_lost failure with its
    # observation and a force-settled cancellation awaiting its teardown.
    survivors = [
        _started("8300", "failed", "owner_lost", termination_observation=OBSERVATION),
        _started(
            "8301",
            "cancelled",
            "issue_cancelled",
            termination_observation=OBSERVATION,
            teardown_unconfirmed_at=TERMINAL,
        ),
        _started("8302", "completed", "completed"),
    ]
    for row in survivors:
        _insert("execution_requests", row)
    command.upgrade(config, REVISION)
    assert _definitions() != prior
    _seed_downgrade_blocker(blocker)

    with pytest.raises(RuntimeError, match="personal_custody_records_present"):
        command.downgrade(config, BELOW)
    assert stamped_revision() == REVISION
    assert CUSTODY in _tables()
    assert _definitions() != prior

    _clear_downgrade_blockers()
    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    assert CUSTODY not in _tables()
    assert _definitions() == prior
    for row in survivors:
        assert sql_rows(
            "SELECT id FROM curie.execution_requests WHERE id = :id", {"id": row["id"]}
        ) == [(row["id"],)]
    # The restored 0159 shapes refuse what 0160 admitted.
    for row in [_relaxed(name) for name in RELAXED]:
        with pytest.raises(IntegrityError):
            _insert("execution_requests", row)


@pytest.mark.parametrize("change", ["shape_missing", "shape_altered", "teardown_altered"])
def test_0160_upgrade_refuses_an_unrecognized_prior_shape(
    isolated_migration_db: IsolatedMigrationDb, change: str
) -> None:
    isolated_migration_db.at(BELOW)
    name = CONSTRAINTS[1] if change == "teardown_altered" else CONSTRAINTS[0]
    sql_rows(f"ALTER TABLE curie.execution_requests DROP CONSTRAINT {name}")
    if change != "shape_missing":
        sql_rows(f"ALTER TABLE curie.execution_requests ADD CONSTRAINT {name} CHECK (true)")
    altered = _definitions()

    with pytest.raises(RuntimeError, match="not the 0159 shape"):
        command.upgrade(alembic_config(), REVISION)

    assert stamped_revision() == BELOW
    assert CUSTODY not in _tables()
    assert _definitions() == altered

"""Migrations 0162 (#5585) and 0167 (#5586) build the factory tracker result schema.

ADR 0277. The upgrade from 0160 adds the two ``agent_versions`` result policy
columns and four tables (grants, deliveries, receipts, transport artifacts)
with named constraints. Every CHECK refuses an insert that violates it alone,
and the error names that constraint, so a probe cannot pass by tripping an
unrelated one; the valid rows each probe mutates are inserted first. The
downgrade to 0160 refuses, before any DDL, while result state or a result
policy exists. With none, it removes exactly the tables and columns, and 0162
applies again.

Migration 0167 (ADR 0277, #5586) adds the request's completion contract, its
policy digest and session reservation, the stored version policy, the grant's
content fingerprint, the delivery origin and the receipt's result comment
URL, and widens the state label and Slack post kind CHECKs. Its tests follow
the 0162 ones at the end of this module, in the same probe style.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config
from alembic import command
from curie_api.config import get_settings
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

REVISION = "0162"
BELOW = "0160"
REFUSAL = "factory_tracker_result_state_present"

GRANTS = "factory_tracker_result_grants"
DELIVERIES = "factory_tracker_result_deliveries"
RECEIPTS = "factory_tracker_result_receipts"
ARTIFACTS = "factory_tracker_result_artifacts"
OUTPUTS = (
    "output_tracker_kind",
    "output_tracker_host",
    "output_scope_id",
    "output_issue_id",
    "output_url",
    "output_body_sha256",
)
COLUMNS = {
    GRANTS: {
        "id",
        "work_item_id",
        "execution_request_id",
        "source_fingerprint",
        "repository_id",
        "allowed_result_kinds",
        "output_identity",
        "implementation_label",
        "expires_at",
        "created_at",
    },
    DELIVERIES: {
        "id",
        "grant_id",
        "execution_request_id",
        "result_kind",
        "reason",
        "proposal_sha256",
        "runtime_epoch",
        "state",
        "attempts",
        "attempt_owner",
        "attempt_lease_expires_at",
        "next_attempt_at",
        "last_error_code",
        "failure_code",
        "create_attempted_at",
        *OUTPUTS,
        "submitted_at",
        "deadline_at",
        "settled_at",
        "created_at",
        "updated_at",
    },
    RECEIPTS: {
        "delivery_id",
        "grant_id",
        "execution_request_id",
        "result_kind",
        "reason",
        "proposal_sha256",
        "revision",
        "result_comment_id",
        *OUTPUTS,
        "output_labels",
        "delivered_at",
    },
    ARTIFACTS: {"delivery_id", "content", "expires_at", "created_at"},
}
AGENT_VERSION_COLUMNS = {"result_policy_schema_version", "result_policy_digest"}
CONSTRAINTS = {
    "agent_versions_result_policy_ck",
    f"{GRANTS}_pkey",
    f"{GRANTS}_execution_request_id_key",
    f"{GRANTS}_output_identity_key",
    f"{GRANTS}_source_fingerprint_ck",
    f"{GRANTS}_repository_id_ck",
    f"{GRANTS}_allowed_result_kinds_ck",
    f"{GRANTS}_output_identity_ck",
    f"{GRANTS}_implementation_label_ck",
    f"{GRANTS}_implementation_label_length_ck",
    f"{DELIVERIES}_pkey",
    f"{DELIVERIES}_grant_id_key",
    f"{DELIVERIES}_result_kind_ck",
    f"{DELIVERIES}_reason_presence_ck",
    f"{DELIVERIES}_reason_ck",
    f"{DELIVERIES}_proposal_sha256_ck",
    f"{DELIVERIES}_output_body_sha256_ck",
    f"{DELIVERIES}_state_ck",
    f"{DELIVERIES}_settled_ck",
    f"{DELIVERIES}_failure_ck",
    f"{DELIVERIES}_last_error_code_ck",
    f"{DELIVERIES}_failure_code_ck",
    f"{DELIVERIES}_attempts_ck",
    f"{DELIVERIES}_lease_ck",
    f"{DELIVERIES}_output_ck",
    f"{DELIVERIES}_deadline_ck",
    f"{RECEIPTS}_pkey",
    f"{RECEIPTS}_reason_ck",
    f"{RECEIPTS}_shape_ck",
    f"{RECEIPTS}_proposal_sha256_ck",
    f"{RECEIPTS}_output_body_sha256_ck",
    f"{RECEIPTS}_revision_ck",
    f"{ARTIFACTS}_pkey",
    f"{ARTIFACTS}_content_ck",
}
INDEXES = {
    f"ix_{GRANTS}_work_item_id",
    f"ix_{DELIVERIES}_owed",
    f"ix_{DELIVERIES}_execution_request_id",
}
JSONB_COLUMNS = {"allowed_result_kinds", "output_labels", "content", "result_policy", "payload"}

SUBMITTED = datetime(2026, 11, 12, 12, 0, tzinfo=UTC)
DEADLINE = SUBMITTED + timedelta(seconds=900)
HEX64 = "a" * 64
REVISION_SHA = "c" * 40

Statement = tuple[str, dict[str, Any]]


def _insert(table: str, values: dict[str, Any]) -> Statement:
    columns = ", ".join(values)
    params = ", ".join(
        f"CAST(:{name} AS jsonb)" if name in JSONB_COLUMNS else f":{name}" for name in values
    )
    return (
        f"INSERT INTO curie.{table} ({columns}) VALUES ({params})",
        {
            name: json.dumps(value) if name in JSONB_COLUMNS and value is not None else value
            for name, value in values.items()
        },
    )


def _on_one_connection[T](work: Callable[[AsyncConnection], Awaitable[T]]) -> T:
    """Run ``work`` on one connection: opening one per statement is slow on a shared host."""

    async def run() -> T:
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        try:
            async with engine.connect() as connection:
                return await work(connection)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _batch(*statements: Statement) -> list[list[dict[str, Any]]]:
    """Statements in one committed transaction, each one's rows as dicts."""

    async def work(connection: AsyncConnection) -> list[list[dict[str, Any]]]:
        results = []
        async with connection.begin():
            for statement, params in statements:
                result = await connection.execute(text(statement), params)
                results.append(
                    [dict(row) for row in result.mappings()] if result.returns_rows else []
                )
        return results

    return _on_one_connection(work)


@dataclass(frozen=True)
class Source:
    """An agent, a WorkItem and its queued execution request."""

    agent_id: uuid.UUID
    work_item_id: uuid.UUID
    request_id: uuid.UUID

    def statements(self, issue: str) -> list[Statement]:
        return [
            _insert("agents", {"id": self.agent_id, "name": f"m0162-{self.agent_id.hex[:8]}"}),
            _insert(
                "work_items",
                {
                    "id": self.work_item_id,
                    "tracker_kind": "github",
                    "tracker_host": "github.com",
                    "tracker_scope_id": "101",
                    "tracker_issue_id": issue,
                    "code_host_kind": "github",
                    "code_host_host": "github.com",
                    "repository_project_id": "101",
                    "code_host_installation_id": 202,
                    "agent_id": self.agent_id,
                    "repository_path": "acme-corp/acme-bot",
                    "conversation_id": f"acme-m0162-{issue}",
                    "version": 1,
                    "next_sequence": 1,
                },
            ),
            _insert(
                "execution_requests",
                {
                    "id": self.request_id,
                    "work_item_id": self.work_item_id,
                    "sequence": 1,
                    "status": "queued",
                    "version": 1,
                    "execution_attempts": 0,
                },
            ),
        ]


def _source() -> Source:
    return Source(uuid.uuid4(), uuid.uuid4(), uuid.uuid4())


def _agent_version(agent_id: uuid.UUID, **policy: Any) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "agent_id": agent_id,
        "version_label": "v1",
        "created_by": "m0162",
        **policy,
    }


def _grant(source: Source, *, ticket: bool) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "work_item_id": source.work_item_id,
        "execution_request_id": source.request_id,
        "source_fingerprint": HEX64,
        "repository_id": "101",
        "allowed_result_kinds": ["findings", "ticket"] if ticket else ["findings"],
        "output_identity": f"execution-request:{source.request_id}",
        "implementation_label": "curie-implement" if ticket else None,
        "expires_at": DEADLINE,
    }


def _outputs() -> dict[str, Any]:
    return {
        "output_tracker_kind": "github",
        "output_tracker_host": "github.com",
        "output_scope_id": "101",
        "output_issue_id": "7101",
        "output_url": "https://github.com/acme-corp/acme-bot/issues/7101",
        "output_body_sha256": "d" * 64,
    }


def _delivery(grant: dict[str, Any], *, ticket: bool) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "grant_id": grant["id"],
        "execution_request_id": grant["execution_request_id"],
        "result_kind": "ticket" if ticket else "findings",
        "reason": None if ticket else "not_reproduced",
        "proposal_sha256": "b" * 64,
        "runtime_epoch": 1,
        "state": "owed",
        "submitted_at": SUBMITTED,
        "deadline_at": DEADLINE,
    }


def _receipt(delivery: dict[str, Any]) -> dict[str, Any]:
    ticket = delivery["result_kind"] == "ticket"
    row = {
        "delivery_id": delivery["id"],
        "grant_id": delivery["grant_id"],
        "execution_request_id": delivery["execution_request_id"],
        "result_kind": delivery["result_kind"],
        "reason": delivery["reason"],
        "proposal_sha256": delivery["proposal_sha256"],
        "revision": REVISION_SHA,
        "result_comment_id": "9001",
    }
    if ticket:
        row.update(_outputs(), output_labels=[])
    return row


def _artifact(delivery: dict[str, Any]) -> dict[str, Any]:
    return {
        "delivery_id": delivery["id"],
        "content": {"kind": delivery["result_kind"]},
        "expires_at": DEADLINE,
    }


@dataclass(frozen=True)
class Seeded:
    """Committed valid rows the probes build on.

    The first source holds a ticket grant, delivery, receipt and artifact; the
    second a findings grant without a delivery; the third no grant at all.
    ``spare_delivery`` is the findings grant's delivery, not yet inserted.
    """

    sources: tuple[Source, Source, Source]
    ticket_delivery: dict[str, Any]
    findings_grant: dict[str, Any]
    spare_delivery: dict[str, Any]


def _seed_valid() -> Seeded:
    first, second, third = _source(), _source(), _source()
    ticket_grant = _grant(first, ticket=True)
    ticket_delivery = _delivery(ticket_grant, ticket=True)
    findings_grant = _grant(second, ticket=False)
    _batch(
        *first.statements("7001"),
        *second.statements("7002"),
        *third.statements("7003"),
        _insert(
            "agent_versions",
            _agent_version(
                first.agent_id, result_policy_schema_version=1, result_policy_digest=HEX64
            ),
        ),
        _insert("agent_versions", _agent_version(second.agent_id)),
        _insert(GRANTS, ticket_grant),
        _insert(GRANTS, findings_grant),
        _insert(DELIVERIES, ticket_delivery),
        _insert(RECEIPTS, _receipt(ticket_delivery)),
        _insert(ARTIFACTS, _artifact(ticket_delivery)),
    )
    spare_delivery = _delivery(findings_grant, ticket=False)
    return Seeded((first, second, third), ticket_delivery, findings_grant, spare_delivery)


@dataclass(frozen=True)
class Bases:
    """The valid rows each probe mutates one field of."""

    grant: dict[str, Any]
    ticket_grant: dict[str, Any]
    delivery: dict[str, Any]
    findings_receipt: dict[str, Any]
    ticket_receipt: dict[str, Any]
    artifact: dict[str, Any]


def _bases(seeded: Seeded) -> Bases:
    grant = _grant(seeded.sources[2], ticket=False)
    delivery = seeded.spare_delivery
    return Bases(
        grant=grant,
        ticket_grant=_grant(seeded.sources[2], ticket=True),
        delivery=delivery,
        findings_receipt=_receipt(delivery),
        ticket_receipt={
            **_receipt({**delivery, "result_kind": "ticket", "reason": None}),
            "delivery_id": delivery["id"],
        },
        artifact=_artifact(delivery),
    )


def _probes(seeded: Seeded) -> dict[str, tuple[Statement, str]]:
    """Each probe violates exactly one named constraint of a row the schema accepts.

    Postgres evaluates CHECKs before unique indexes and foreign keys, so a
    probe whose base row would also collide on a key still reports its CHECK.
    """

    agent_id = seeded.sources[1].agent_id
    bases = _bases(seeded)
    grant, ticket_grant, delivery = bases.grant, bases.ticket_grant, bases.delivery
    findings_receipt, ticket_receipt = bases.findings_receipt, bases.ticket_receipt
    settled = {"settled_at": SUBMITTED + timedelta(seconds=60)}

    def policy(**values: Any) -> Statement:
        return _insert("agent_versions", _agent_version(agent_id, **values))

    def grants(**values: Any) -> Statement:
        return _insert(GRANTS, {**grant, **values})

    def deliveries(**values: Any) -> Statement:
        return _insert(DELIVERIES, {**delivery, **values})

    def receipts(base: dict[str, Any], **values: Any) -> Statement:
        return _insert(RECEIPTS, {**base, **values})

    policy_ck = "agent_versions_result_policy_ck"
    return {
        "policy_digest_missing": (policy(result_policy_schema_version=1), policy_ck),
        "policy_version_missing": (policy(result_policy_digest=HEX64), policy_ck),
        "policy_version_2": (
            policy(result_policy_schema_version=2, result_policy_digest=HEX64),
            policy_ck,
        ),
        "policy_digest_not_hex": (
            policy(result_policy_schema_version=1, result_policy_digest="A" * 64),
            policy_ck,
        ),
        "grant_fingerprint": (grants(source_fingerprint="xyz"), f"{GRANTS}_source_fingerprint_ck"),
        "grant_repository_blank": (grants(repository_id="  "), f"{GRANTS}_repository_id_ck"),
        "grant_kinds_ticket_only": (
            _insert(GRANTS, {**ticket_grant, "allowed_result_kinds": ["ticket"]}),
            f"{GRANTS}_allowed_result_kinds_ck",
        ),
        "grant_kinds_unsorted": (
            _insert(GRANTS, {**ticket_grant, "allowed_result_kinds": ["ticket", "findings"]}),
            f"{GRANTS}_allowed_result_kinds_ck",
        ),
        "grant_identity_shape": (
            grants(output_identity="-execution-request"),
            f"{GRANTS}_output_identity_ck",
        ),
        "grant_identity_length": (
            grants(output_identity="a" * 129),
            f"{GRANTS}_output_identity_ck",
        ),
        "grant_findings_with_label": (
            grants(implementation_label="curie-implement"),
            f"{GRANTS}_implementation_label_ck",
        ),
        "grant_ticket_without_label": (
            _insert(GRANTS, {**ticket_grant, "implementation_label": None}),
            f"{GRANTS}_implementation_label_ck",
        ),
        "grant_label_empty": (
            _insert(GRANTS, {**ticket_grant, "implementation_label": ""}),
            f"{GRANTS}_implementation_label_length_ck",
        ),
        "grant_duplicate_request": (
            grants(execution_request_id=seeded.sources[1].request_id),
            f"{GRANTS}_execution_request_id_key",
        ),
        "grant_duplicate_identity": (
            grants(output_identity=seeded.findings_grant["output_identity"]),
            f"{GRANTS}_output_identity_key",
        ),
        "delivery_kind": (
            deliveries(result_kind="patch", reason=None),
            f"{DELIVERIES}_result_kind_ck",
        ),
        "delivery_findings_without_reason": (
            deliveries(reason=None),
            f"{DELIVERIES}_reason_presence_ck",
        ),
        "delivery_ticket_with_reason": (
            deliveries(result_kind="ticket"),
            f"{DELIVERIES}_reason_presence_ck",
        ),
        "delivery_reason_unknown": (deliveries(reason="made_up"), f"{DELIVERIES}_reason_ck"),
        "delivery_proposal_digest": (
            deliveries(proposal_sha256="B" * 64),
            f"{DELIVERIES}_proposal_sha256_ck",
        ),
        "delivery_output_digest": (
            deliveries(**{**_outputs(), "output_body_sha256": "not-a-digest"}),
            f"{DELIVERIES}_output_body_sha256_ck",
        ),
        "delivery_state_unknown": (
            deliveries(state="pending", **settled),
            f"{DELIVERIES}_state_ck",
        ),
        "delivery_delivered_unsettled": (deliveries(state="delivered"), f"{DELIVERIES}_settled_ck"),
        "delivery_owed_settled": (deliveries(**settled), f"{DELIVERIES}_settled_ck"),
        "delivery_failed_without_code": (
            deliveries(state="failed", **settled),
            f"{DELIVERIES}_failure_ck",
        ),
        "delivery_delivered_with_failure": (
            deliveries(state="delivered", failure_code="output_ambiguous", **settled),
            f"{DELIVERIES}_failure_ck",
        ),
        "delivery_last_error_free_text": (
            deliveries(last_error_code="Tracker said: boom"),
            f"{DELIVERIES}_last_error_code_ck",
        ),
        "delivery_failure_free_text": (
            deliveries(state="failed", failure_code="the title was rude", **settled),
            f"{DELIVERIES}_failure_code_ck",
        ),
        "delivery_attempts_negative": (deliveries(attempts=-1), f"{DELIVERIES}_attempts_ck"),
        "delivery_owner_without_lease": (
            deliveries(attempt_owner="worker-1"),
            f"{DELIVERIES}_lease_ck",
        ),
        "delivery_lease_without_owner": (
            deliveries(attempt_lease_expires_at=DEADLINE),
            f"{DELIVERIES}_lease_ck",
        ),
        "delivery_partial_output": (
            deliveries(output_url="https://github.com/acme-corp/acme-bot/issues/7101"),
            f"{DELIVERIES}_output_ck",
        ),
        "delivery_deadline_not_after_submission": (
            deliveries(deadline_at=SUBMITTED),
            f"{DELIVERIES}_deadline_ck",
        ),
        "delivery_duplicate_grant": (
            _insert(DELIVERIES, {**seeded.ticket_delivery, "id": uuid.uuid4()}),
            f"{DELIVERIES}_grant_id_key",
        ),
        "receipt_reason_unknown": (
            receipts(findings_receipt, reason="made_up"),
            f"{RECEIPTS}_reason_ck",
        ),
        "receipt_kind_unknown": (
            receipts(findings_receipt, result_kind="patch"),
            f"{RECEIPTS}_shape_ck",
        ),
        "receipt_findings_with_output": (
            receipts(findings_receipt, output_url="https://github.com/acme-corp/acme-bot/issues/1"),
            f"{RECEIPTS}_shape_ck",
        ),
        "receipt_findings_without_reason": (
            receipts(findings_receipt, reason=None),
            f"{RECEIPTS}_shape_ck",
        ),
        "receipt_ticket_without_labels": (
            receipts(ticket_receipt, output_labels=None),
            f"{RECEIPTS}_shape_ck",
        ),
        "receipt_ticket_with_reason": (
            receipts(ticket_receipt, reason="not_reproduced"),
            f"{RECEIPTS}_shape_ck",
        ),
        "receipt_proposal_digest": (
            receipts(findings_receipt, proposal_sha256="xyz"),
            f"{RECEIPTS}_proposal_sha256_ck",
        ),
        "receipt_output_digest": (
            receipts(ticket_receipt, output_body_sha256="D" * 64),
            f"{RECEIPTS}_output_body_sha256_ck",
        ),
        "receipt_revision": (receipts(findings_receipt, revision="abc"), f"{RECEIPTS}_revision_ck"),
        "artifact_not_object": (
            _insert(ARTIFACTS, {**bases.artifact, "content": ["kind"]}),
            f"{ARTIFACTS}_content_ck",
        ),
    }


def _probe(
    probes: dict[str, tuple[Statement, str]], prerequisite: list[Statement]
) -> dict[str, str]:
    """Run each probe in its own savepoint over ``prerequisite``; all of it rolls back.

    Returns, per probe, the refusing constraint's message (or "accepted").
    """

    async def work(connection: AsyncConnection) -> dict[str, str]:
        outcomes: dict[str, str] = {}
        transaction = await connection.begin()
        try:
            for statement, params in prerequisite:
                await connection.execute(text(statement), params)
            for name, ((statement, params), _) in probes.items():
                savepoint = await connection.begin_nested()
                try:
                    await connection.execute(text(statement), params)
                    outcomes[name] = "accepted"
                except IntegrityError as error:
                    outcomes[name] = str(error.orig)
                finally:
                    await savepoint.rollback()
        finally:
            await transaction.rollback()
        return outcomes

    return _on_one_connection(work)


_CATALOG: list[Statement] = [
    ("SELECT table_name FROM information_schema.tables WHERE table_schema = 'curie'", {}),
    (
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND (table_name LIKE 'factory_tracker_result_%' "
        "OR table_name = 'agent_versions')",
        {},
    ),
    (
        "SELECT c.conname FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace "
        "WHERE n.nspname = 'curie' AND c.contype IN ('c', 'u', 'p') "
        "AND (c.conname LIKE 'factory_tracker_result_%' "
        "OR c.conname = 'agent_versions_result_policy_ck')",
        {},
    ),
    (
        "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'curie' "
        "AND indexname LIKE 'ix_factory_tracker_result_%'",
        {},
    ),
    ("SELECT version_num FROM curie.alembic_version", {}),
]


@dataclass(frozen=True)
class Catalog:
    tables: set[str]
    columns: dict[str, set[str]]
    constraints: set[str]
    indexes: dict[str, str]
    revision: str

    @property
    def present(self) -> bool:
        return (
            all(self.columns.get(table) == columns for table, columns in COLUMNS.items())
            and AGENT_VERSION_COLUMNS <= self.columns.get("agent_versions", set())
            and self.constraints == CONSTRAINTS
            and set(self.indexes) == INDEXES
        )

    @property
    def absent(self) -> bool:
        return (
            not set(COLUMNS) & self.tables
            and not AGENT_VERSION_COLUMNS & self.columns.get("agent_versions", set())
            and not self.constraints
            and not self.indexes
        )


def _catalog(*extra: Statement) -> tuple[Catalog, list[list[dict[str, Any]]]]:
    tables, columns, constraints, indexes, (stamp,), *rest = _batch(*_CATALOG, *extra)
    by_table: dict[str, set[str]] = {}
    for row in columns:
        by_table.setdefault(row["table_name"], set()).add(row["column_name"])
    return (
        Catalog(
            {row["table_name"] for row in tables},
            by_table,
            {row["conname"] for row in constraints},
            {row["indexname"]: row["indexdef"] for row in indexes},
            stamp["version_num"],
        ),
        rest,
    )


def test_0162_upgrades_from_0160_with_every_named_table_column_and_constraint(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    source = _source()
    version = _agent_version(source.agent_id)
    _batch(*source.statements("7001"), _insert("agent_versions", version))
    catalog, _ = _catalog()
    assert catalog.absent and catalog.revision == BELOW, catalog

    command.upgrade(config, REVISION)

    catalog, (versions,) = _catalog(
        (
            "SELECT id, result_policy_schema_version, result_policy_digest "
            "FROM curie.agent_versions",
            {},
        )
    )
    assert catalog.present and catalog.revision == REVISION, catalog
    assert "WHERE (state = 'owed'::text)" in catalog.indexes[f"ix_{DELIVERIES}_owed"]
    # An existing agent version has no result policy: the implementation contract.
    assert versions == [
        {"id": version["id"], "result_policy_schema_version": None, "result_policy_digest": None}
    ]


def test_0162_each_check_refuses_the_one_violation_it_names(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    seeded = _seed_valid()
    probes = _probes(seeded)
    # Every CHECK of the four tables and the agent version policy has a probe.
    checks = {name for name in CONSTRAINTS if name.endswith("_ck")}
    assert checks <= {constraint for _, constraint in probes.values()}

    # The unmutated rows the probes start from are each accepted.
    bases = _bases(seeded)
    assert _probe(
        {
            "grant": (_insert(GRANTS, bases.grant), ""),
            "ticket_grant": (_insert(GRANTS, bases.ticket_grant), ""),
            "delivery": (_insert(DELIVERIES, bases.delivery), ""),
        },
        [],
    ) == {"grant": "accepted", "ticket_grant": "accepted", "delivery": "accepted"}
    # Receipt and artifact rows need the delivery they reference.
    prerequisite = [_insert(DELIVERIES, bases.delivery)]
    assert _probe(
        {
            "findings_receipt": (_insert(RECEIPTS, bases.findings_receipt), ""),
            "ticket_receipt": (_insert(RECEIPTS, bases.ticket_receipt), ""),
            "artifact": (_insert(ARTIFACTS, bases.artifact), ""),
        },
        prerequisite,
    ) == {"findings_receipt": "accepted", "ticket_receipt": "accepted", "artifact": "accepted"}

    outcomes = _probe(probes, prerequisite)
    refused_by = {
        probe: next(
            (constraint for constraint in sorted(CONSTRAINTS) if f'"{constraint}"' in outcome),
            outcome,
        )
        for probe, outcome in outcomes.items()
    }
    assert refused_by == {name: constraint for name, (_, constraint) in probes.items()}


def _grant_rows() -> list[Statement]:
    source = _source()
    return [*source.statements("7101"), _insert(GRANTS, _grant(source, ticket=False))]


def _delivery_rows() -> list[Statement]:
    source = _source()
    grant = _grant(source, ticket=False)
    return [
        *source.statements("7102"),
        _insert(GRANTS, grant),
        _insert(DELIVERIES, _delivery(grant, ticket=False)),
    ]


def _receipt_rows() -> list[Statement]:
    source = _source()
    grant = _grant(source, ticket=True)
    delivery = {
        **_delivery(grant, ticket=True),
        **_outputs(),
        "state": "delivered",
        "settled_at": SUBMITTED + timedelta(seconds=60),
    }
    return [
        *source.statements("7103"),
        _insert(GRANTS, grant),
        _insert(DELIVERIES, delivery),
        _insert(RECEIPTS, _receipt(delivery)),
    ]


def _artifact_rows() -> list[Statement]:
    source = _source()
    grant = _grant(source, ticket=False)
    delivery = _delivery(grant, ticket=False)
    return [
        *source.statements("7104"),
        _insert(GRANTS, grant),
        _insert(DELIVERIES, delivery),
        _insert(ARTIFACTS, _artifact(delivery)),
    ]


def _policy_rows() -> list[Statement]:
    source = _source()
    return [
        *source.statements("7105"),
        _insert(
            "agent_versions",
            _agent_version(
                source.agent_id, result_policy_schema_version=1, result_policy_digest=HEX64
            ),
        ),
    ]


SEEDS = {
    "grant": _grant_rows,
    "delivery": _delivery_rows,
    "receipt": _receipt_rows,
    "artifact": _artifact_rows,
    "policy": _policy_rows,
}
_STATE: list[Statement] = [
    *(
        (f"SELECT * FROM curie.{table} ORDER BY 1", {})
        for table in (GRANTS, DELIVERIES, RECEIPTS, ARTIFACTS)
    ),
    (
        "SELECT id, result_policy_schema_version, result_policy_digest "
        "FROM curie.agent_versions ORDER BY id",
        {},
    ),
]


@pytest.mark.parametrize("seed", sorted(SEEDS))
def test_0162_downgrade_refuses_while_result_state_or_a_policy_exists(
    isolated_migration_db: IsolatedMigrationDb, seed: str
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    _batch(*SEEDS[seed]())
    catalog, before = _catalog(*_STATE)
    assert catalog.present and catalog.revision == REVISION, catalog
    *tables, versions = before
    assert any(tables) or any(row["result_policy_digest"] for row in versions), before

    with pytest.raises(RuntimeError, match=REFUSAL):
        command.downgrade(config, BELOW)

    # Refused before any DDL: the revision, schema and rows are as they were.
    # The private database is dropped after the test, so nothing is restored.
    catalog, after = _catalog(*_STATE)
    assert catalog.present and catalog.revision == REVISION, catalog
    assert after == before


def test_0162_downgrade_without_state_removes_the_tables_and_columns_and_reapplies(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    # A WorkItem, its request and an agent version without a policy are not result state.
    source = _source()
    version = _agent_version(source.agent_id)
    _batch(*source.statements("7201"), _insert("agent_versions", version))
    catalog, _ = _catalog()
    assert catalog.present and catalog.revision == REVISION, catalog

    command.downgrade(config, BELOW)

    catalog, (versions, requests) = _catalog(
        ("SELECT id FROM curie.agent_versions", {}),
        ("SELECT id FROM curie.execution_requests", {}),
    )
    assert catalog.absent and catalog.revision == BELOW, catalog
    assert versions == [{"id": version["id"]}]
    assert requests == [{"id": source.request_id}]

    command.upgrade(config, REVISION)

    catalog, (versions,) = _catalog(
        (
            "SELECT id, result_policy_schema_version, result_policy_digest "
            "FROM curie.agent_versions",
            {},
        )
    )
    assert catalog.present and catalog.revision == REVISION, catalog
    assert versions == [
        {"id": version["id"], "result_policy_schema_version": None, "result_policy_digest": None}
    ]


# Migration 0167 (#5586) --------------------------------------------------------------

REVISION_0167 = "0167"
REFUSAL_0167 = "factory_triage_activation_state_present"
NEW_COLUMNS_0167 = {
    "execution_requests": {
        "completion_contract",
        "result_policy_digest",
        "model_session_reserved_at",
    },
    "agent_versions": {"result_policy"},
    GRANTS: {"source_content_sha256"},
    DELIVERIES: {"origin"},
    RECEIPTS: {"result_comment_url"},
}
CONSTRAINTS_0167 = {
    "execution_requests_completion_contract_ck",
    "execution_requests_result_policy_digest_ck",
    "execution_requests_result_policy_digest_format_ck",
    "execution_requests_model_session_reserved_at_ck",
    "agent_versions_result_policy_presence_ck",
    "agent_versions_result_policy_shape_ck",
    f"{GRANTS}_source_content_sha256_ck",
    f"{DELIVERIES}_origin_ck",
    f"{DELIVERIES}_platform_origin_ck",
    f"{DELIVERIES}_budget_exhausted_origin_ck",
}
NOTICES = "factory_terminal_notices"
SLACK_THREADS = "factory_slack_threads"
SLACK_POSTS = "factory_slack_posts"
TRIAGED = "curie-factory:triaged"
FINDINGS_POLICY = {"results": ["findings"], "implementation_label": None}
TICKET_POLICY = {"results": ["findings", "ticket"], "implementation_label": "curie-implement"}


def _sql_list(names: Any) -> str:
    return ", ".join(f"'{name}'" for name in sorted(names))


def _columns_0167() -> dict[str, set[str]]:
    rows = _batch(
        (
            "SELECT table_name, column_name FROM information_schema.columns "
            f"WHERE table_schema = 'curie' AND table_name IN ({_sql_list(NEW_COLUMNS_0167)})",
            {},
        )
    )[0]
    found: dict[str, set[str]] = {}
    for row in rows:
        found.setdefault(row["table_name"], set()).add(row["column_name"])
    return {table: found.get(table, set()) & columns for table, columns in NEW_COLUMNS_0167.items()}


def _constraints_0167() -> set[str]:
    (rows,) = _batch(
        (
            "SELECT c.conname FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace "
            f"WHERE n.nspname = 'curie' AND c.conname IN ({_sql_list(CONSTRAINTS_0167)})",
            {},
        )
    )
    return {row["conname"] for row in rows}


def _stamp() -> str:
    (rows,) = _batch(("SELECT version_num FROM curie.alembic_version", {}))
    return str(rows[0]["version_num"])


def _request_row(source: Source, **values: Any) -> dict[str, Any]:
    """A second, queued request on ``source``'s WorkItem."""

    return {
        "id": uuid.uuid4(),
        "work_item_id": source.work_item_id,
        "sequence": 2,
        "status": "queued",
        "version": 1,
        "execution_attempts": 0,
        **values,
    }


def _notice(source: Source, label: str | None) -> dict[str, Any]:
    return {
        "execution_request_id": source.request_id,
        "work_item_id": source.work_item_id,
        "applied_label": label,
    }


def _slack_thread(source: Source) -> dict[str, Any]:
    return {
        "work_item_id": source.work_item_id,
        "agent_id": source.agent_id,
        "channel_identity": "T0ACME",
        "channel": "C0ACME",
        "first_sequence": 1,
    }


def _slack_post(source: Source, kind: str) -> dict[str, Any]:
    return {
        "work_item_id": source.work_item_id,
        "post_key": f"{kind}:{source.request_id}",
        "kind": kind,
        "sequence": 1,
        "rank": 1,
        "ordinal": 1,
        "payload": {"text": "Triage reported: not reproduced."},
    }


def _source_rows(issue: str) -> tuple[Source, list[Statement]]:
    """A source whose agent, WorkItem and queued request are valid at 0162 and 0167."""

    source = _source()
    return source, source.statements(issue)


def test_0167_upgrades_from_0162_and_existing_rows_keep_the_implementation_contract(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION)
    source, rows = _source_rows("7301")
    grant = _grant(source, ticket=False)
    delivery = _delivery(grant, ticket=False)
    version = _agent_version(source.agent_id)
    _batch(
        *rows,
        _insert("agent_versions", version),
        _insert(GRANTS, grant),
        _insert(DELIVERIES, delivery),
    )
    assert all(not columns for columns in _columns_0167().values())
    assert not _constraints_0167()

    command.upgrade(config, REVISION_0167)

    assert _stamp() == REVISION_0167
    assert _columns_0167() == NEW_COLUMNS_0167
    assert _constraints_0167() == CONSTRAINTS_0167
    requests, versions, grants, deliveries = _batch(
        (
            "SELECT completion_contract, result_policy_digest, model_session_reserved_at "
            "FROM curie.execution_requests",
            {},
        ),
        ("SELECT result_policy FROM curie.agent_versions", {}),
        (f"SELECT source_content_sha256 FROM curie.{GRANTS}", {}),
        (f"SELECT origin FROM curie.{DELIVERIES}", {}),
    )
    assert requests == [
        {
            "completion_contract": "implementation",
            "result_policy_digest": None,
            "model_session_reserved_at": None,
        }
    ]
    assert versions == [{"result_policy": None}]
    assert grants == [{"source_content_sha256": None}]
    assert deliveries == [{"origin": "submitted"}]


def test_0167_upgrades_rows_the_0162_schema_admits_and_backfills_platform_origin(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """A 0162 policy digest and a 0162 ``budget_exhausted`` delivery do not block 0167.

    The digest stays (its declaration is only in the bundle bytes) with no
    policy; the ``budget_exhausted`` delivery becomes ``platform``; ordinary
    rows keep the defaults. Afterwards the CHECKs still refuse a policy with no
    digest and a submitted ``budget_exhausted`` delivery.
    """

    config = alembic_config()
    isolated_migration_db.at(REVISION)
    declared_source, declared_rows = _source_rows("7311")
    plain_source, plain_rows = _source_rows("7312")
    declared = _agent_version(
        declared_source.agent_id, result_policy_schema_version=1, result_policy_digest=HEX64
    )
    plain = _agent_version(plain_source.agent_id)
    budget_grant = _grant(declared_source, ticket=False)
    budget = {**_delivery(budget_grant, ticket=False), "reason": "budget_exhausted"}
    ticket_grant = _grant(plain_source, ticket=True)
    ticket = _delivery(ticket_grant, ticket=True)
    _batch(
        *declared_rows,
        *plain_rows,
        _insert("agent_versions", declared),
        _insert("agent_versions", plain),
        _insert(GRANTS, budget_grant),
        _insert(GRANTS, ticket_grant),
        _insert(DELIVERIES, budget),
        _insert(DELIVERIES, ticket),
    )

    command.upgrade(config, REVISION_0167)

    assert _stamp() == REVISION_0167
    assert _constraints_0167() == CONSTRAINTS_0167
    versions, deliveries = _batch(
        (
            "SELECT id, result_policy_schema_version, result_policy_digest, result_policy "
            "FROM curie.agent_versions",
            {},
        ),
        (f"SELECT id, reason, origin FROM curie.{DELIVERIES}", {}),
    )
    assert {row["id"]: row for row in versions} == {
        declared["id"]: {
            "id": declared["id"],
            "result_policy_schema_version": 1,
            "result_policy_digest": HEX64,
            "result_policy": None,
        },
        plain["id"]: {
            "id": plain["id"],
            "result_policy_schema_version": None,
            "result_policy_digest": None,
            "result_policy": None,
        },
    }
    assert {row["id"]: (row["reason"], row["origin"]) for row in deliveries} == {
        budget["id"]: ("budget_exhausted", "platform"),
        ticket["id"]: (None, "submitted"),
    }

    other, other_rows = _source_rows("7313")
    other_grant = _grant(other, ticket=False)
    outcomes = _probe(
        {
            "policy_without_digest": (
                _insert(
                    "agent_versions", _agent_version(other.agent_id, result_policy=FINDINGS_POLICY)
                ),
                "",
            ),
            "submitted_budget_exhausted": (
                _insert(
                    DELIVERIES,
                    {**_delivery(other_grant, ticket=False), "reason": "budget_exhausted"},
                ),
                "",
            ),
            "platform_budget_exhausted": (
                _insert(
                    DELIVERIES,
                    {
                        **_delivery(other_grant, ticket=False),
                        "reason": "budget_exhausted",
                        "origin": "platform",
                    },
                ),
                "",
            ),
        },
        [*other_rows, _insert(GRANTS, other_grant)],
    )
    assert '"agent_versions_result_policy_presence_ck"' in outcomes["policy_without_digest"]
    assert f'"{DELIVERIES}_budget_exhausted_origin_ck"' in outcomes["submitted_budget_exhausted"]
    assert outcomes["platform_budget_exhausted"] == "accepted"


def _probes_0167(
    source: Source, grant: dict[str, Any], other: Source
) -> dict[str, tuple[Statement, str]]:
    """Each probe violates exactly the named 0167 constraint of a row the schema accepts.

    ``grant`` is ``source``'s committed findings grant, which the delivery
    probes deliver; the grant probes mint one for ``other``, whose request no
    grant holds yet. Postgres evaluates CHECKs in constraint name order, so the
    unknown origin probe, which also fails the platform shape, reports
    ``origin_ck``.
    """

    reserved = {"model_session_reserved_at": SUBMITTED}
    tracker_result = {"completion_contract": "tracker_result", "result_policy_digest": HEX64}
    findings = _delivery(grant, ticket=False)
    budget = {"reason": "budget_exhausted"}

    def version(**policy: Any) -> Statement:
        return _insert("agent_versions", _agent_version(source.agent_id, **policy))

    def declared(policy: Any) -> Statement:
        return version(
            result_policy_schema_version=1, result_policy_digest=HEX64, result_policy=policy
        )

    def request(**values: Any) -> Statement:
        return _insert("execution_requests", _request_row(source, **values))

    shape = "agent_versions_result_policy_shape_ck"
    return {
        "contract_unknown": (
            request(completion_contract="investigation"),
            "execution_requests_completion_contract_ck",
        ),
        "tracker_result_without_digest": (
            request(completion_contract="tracker_result"),
            "execution_requests_result_policy_digest_ck",
        ),
        "implementation_with_digest": (
            request(result_policy_digest=HEX64),
            "execution_requests_result_policy_digest_ck",
        ),
        "digest_not_hex64": (
            request(completion_contract="tracker_result", result_policy_digest="A" * 64),
            "execution_requests_result_policy_digest_format_ck",
        ),
        "implementation_reserves_a_session": (
            request(**reserved),
            "execution_requests_model_session_reserved_at_ck",
        ),
        "policy_without_digest": (
            version(result_policy=FINDINGS_POLICY),
            "agent_versions_result_policy_presence_ck",
        ),
        "policy_ticket_only": (
            declared({"results": ["ticket"], "implementation_label": "curie-implement"}),
            shape,
        ),
        "policy_unsorted": (
            declared({"results": ["ticket", "findings"], "implementation_label": "x"}),
            shape,
        ),
        "policy_extra_key": (declared({**FINDINGS_POLICY, "contract": "tracker_result"}), shape),
        "policy_missing_label_key": (declared({"results": ["findings"]}), shape),
        "policy_label_without_ticket": (
            declared({"results": ["findings"], "implementation_label": "curie-implement"}),
            shape,
        ),
        "policy_ticket_without_label": (
            declared({"results": ["findings", "ticket"], "implementation_label": None}),
            shape,
        ),
        "policy_label_too_long": (
            declared({"results": ["findings", "ticket"], "implementation_label": "x" * 51}),
            shape,
        ),
        "policy_not_an_object": (declared(["findings"]), shape),
        "content_digest_not_hex64": (
            _insert(GRANTS, {**_grant(other, ticket=False), "source_content_sha256": "Z" * 64}),
            f"{GRANTS}_source_content_sha256_ck",
        ),
        "origin_unknown": (
            _insert(DELIVERIES, {**findings, "origin": "runner"}),
            f"{DELIVERIES}_origin_ck",
        ),
        "platform_not_budget_exhausted": (
            _insert(DELIVERIES, {**findings, "origin": "platform"}),
            f"{DELIVERIES}_platform_origin_ck",
        ),
        "submitted_budget_exhausted": (
            _insert(DELIVERIES, {**findings, **budget}),
            f"{DELIVERIES}_budget_exhausted_origin_ck",
        ),
        "label_unknown": (
            _insert(NOTICES, _notice(source, "curie-factory:investigated")),
            f"{NOTICES}_applied_label_ck",
        ),
        "slack_kind_unknown": (
            _insert(SLACK_POSTS, _slack_post(source, "investigation")),
            "factory_slack_posts_kind_ck",
        ),
    } | {
        # Liveness: the valid rows each probe mutates one field of.
        "valid_tracker_result_request": (request(**tracker_result, **reserved), ""),
        "valid_findings_policy": (declared(FINDINGS_POLICY), ""),
        # A 0162 digest whose declaration SQL cannot backfill; admission
        # refuses it as an unreadable policy, the schema keeps it.
        "valid_digest_without_policy": (
            version(result_policy_schema_version=1, result_policy_digest=HEX64),
            "",
        ),
        "valid_ticket_policy": (declared(TICKET_POLICY), ""),
        "valid_content_digest": (
            _insert(GRANTS, {**_grant(other, ticket=False), "source_content_sha256": HEX64}),
            "",
        ),
        "valid_platform_findings": (
            _insert(DELIVERIES, {**findings, **budget, "origin": "platform"}),
            "",
        ),
        "valid_submitted_findings": (_insert(DELIVERIES, findings), ""),
        "valid_triaged_label": (_insert(NOTICES, _notice(source, TRIAGED)), ""),
        "valid_triage_post": (_insert(SLACK_POSTS, _slack_post(source, "triage")), ""),
        "valid_request_settling": (
            _insert(DELIVERIES, {**findings, "last_error_code": "request_settling"}),
            "",
        ),
    }


def test_0167_each_check_refuses_the_one_violation_it_names_and_admits_the_valid_row(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION_0167)
    source, rows = _source_rows("7302")
    other, other_rows = _source_rows("7303")
    grant = _grant(source, ticket=False)
    _batch(
        *rows,
        *other_rows,
        _insert(GRANTS, grant),
        _insert(SLACK_THREADS, _slack_thread(source)),
    )
    probes = _probes_0167(source, grant, other)
    named = {constraint for _, constraint in probes.values() if constraint}
    assert CONSTRAINTS_0167 <= named

    outcomes = _probe(probes, [])
    every = CONSTRAINTS_0167 | {f"{NOTICES}_applied_label_ck", "factory_slack_posts_kind_ck"}
    refused_by = {
        probe: (
            "accepted"
            if outcome == "accepted"
            else next((c for c in sorted(every) if f'"{c}"' in outcome), outcome)
        )
        for probe, outcome in outcomes.items()
    }
    assert refused_by == {
        name: constraint or "accepted" for name, (_, constraint) in probes.items()
    }


def test_the_triaged_label_and_triage_post_are_refused_at_0162(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    source, rows = _source_rows("7304")
    _batch(*rows, _insert(SLACK_THREADS, _slack_thread(source)))

    outcomes = _probe(
        {
            "triaged": (_insert(NOTICES, _notice(source, TRIAGED)), ""),
            "triage_post": (_insert(SLACK_POSTS, _slack_post(source, "triage")), ""),
            "needs_human": (_insert(NOTICES, _notice(source, "curie-factory:needs-human")), ""),
            "terminal_post": (_insert(SLACK_POSTS, _slack_post(source, "terminal")), ""),
        },
        [],
    )

    assert f'"{NOTICES}_applied_label_ck"' in outcomes["triaged"], outcomes
    assert '"factory_slack_posts_kind_ck"' in outcomes["triage_post"], outcomes
    assert outcomes["needs_human"] == "accepted"
    assert outcomes["terminal_post"] == "accepted"


def _state_0167(kind: str) -> list[Statement]:
    """Rows only 0167 can represent, one kind at a time."""

    source, rows = _source_rows(f"74{len(kind):02d}")
    grant = _grant(source, ticket=False)
    if kind == "tracker_result_request":
        return [
            *rows,
            _insert(
                "execution_requests",
                _request_row(
                    source, completion_contract="tracker_result", result_policy_digest=HEX64
                ),
            ),
        ]
    if kind == "result_policy":
        return [
            *rows,
            _insert(
                "agent_versions",
                _agent_version(
                    source.agent_id,
                    result_policy_schema_version=1,
                    result_policy_digest=HEX64,
                    result_policy=FINDINGS_POLICY,
                ),
            ),
        ]
    if kind == "content_digest":
        return [*rows, _insert(GRANTS, {**grant, "source_content_sha256": HEX64})]
    if kind == "platform_delivery":
        delivery = {
            **_delivery(grant, ticket=False),
            "reason": "budget_exhausted",
            "origin": "platform",
        }
        return [*rows, _insert(GRANTS, grant), _insert(DELIVERIES, delivery)]
    if kind == "triaged_label":
        return [*rows, _insert(NOTICES, _notice(source, TRIAGED))]
    assert kind == "triage_post"
    return [
        *rows,
        _insert(SLACK_THREADS, _slack_thread(source)),
        _insert(SLACK_POSTS, _slack_post(source, "triage")),
    ]


STATE_KINDS_0167 = (
    "tracker_result_request",
    "result_policy",
    "content_digest",
    "platform_delivery",
    "triaged_label",
    "triage_post",
)


@pytest.mark.parametrize("kind", STATE_KINDS_0167)
def test_0167_downgrade_refuses_while_state_only_0167_holds_exists(
    isolated_migration_db: IsolatedMigrationDb, kind: str
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION_0167)
    _batch(*_state_0167(kind))

    with pytest.raises(RuntimeError, match=REFUSAL_0167):
        command.downgrade(config, REVISION)

    # Refused before any DDL.
    assert _stamp() == REVISION_0167
    assert _columns_0167() == NEW_COLUMNS_0167
    assert _constraints_0167() == CONSTRAINTS_0167


def test_0167_downgrade_without_state_restores_0162_and_reapplies(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(REVISION_0167)
    source, rows = _source_rows("7305")
    grant = _grant(source, ticket=False)
    _batch(
        *rows,
        _insert(GRANTS, grant),
        _insert(DELIVERIES, _delivery(grant, ticket=False)),
        _insert(NOTICES, _notice(source, "curie-factory:needs-human")),
    )

    command.downgrade(config, REVISION)

    assert _stamp() == REVISION
    assert all(not columns for columns in _columns_0167().values())
    assert not _constraints_0167()
    # The 0162 label CHECK is back: the triaged label is refused again.
    outcome = _probe({"triaged": (_insert(NOTICES, _notice(_source(), TRIAGED)), "")}, [])
    assert f'"{NOTICES}_applied_label_ck"' in outcome["triaged"], outcome

    command.upgrade(config, REVISION_0167)

    assert _stamp() == REVISION_0167
    assert _columns_0167() == NEW_COLUMNS_0167
    assert _constraints_0167() == CONSTRAINTS_0167
    (deliveries,) = _batch((f"SELECT origin FROM curie.{DELIVERIES}", {}))
    assert deliveries == [{"origin": "submitted"}]

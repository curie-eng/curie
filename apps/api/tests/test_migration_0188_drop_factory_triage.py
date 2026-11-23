"""Migration 0188 drops the factory triage schema (#5807).

Real Postgres, one private database per test. Every test starts from a clone
at 0186, the revision below, seeds rows the way the releases that held the
triage path wrote them, and upgrades through Alembic. The upgrade drops the six
triage tables, three ``execution_requests`` columns and three
``agent_versions`` columns, removes ``triage`` from the Slack post kinds and
``curie-factory:triaged`` from the state labels, keeps every row of every kept
table, and settles retained triage requests for the Slack progress lane. The
downgrade restores the 0186 shape with no rows.
"""

from __future__ import annotations

import asyncio
import json
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

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
from alembic.script import ScriptDirectory
from channel_protocol.reply import ReplyAck, ReplyEvent, ReplyPost, ReplyUpdate
from curie_api.config import get_settings
from curie_api.schema_compat import (
    KIND_CONTRACT,
    AppWindow,
    load_kinds,
    load_window,
    plan_upgrade,
)
from curie_worker.factory_slack_progress import (
    FactorySlackProgressLoop,
    PostgresFactorySlackStore,
    delivery_id_for,
)
from curie_worker.reply_sink import TargetRoute
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool

REVISION = "0188"
BELOW = "0186"

GRANTS = "factory_tracker_result_grants"
DELIVERIES = "factory_tracker_result_deliveries"
RECEIPTS = "factory_tracker_result_receipts"
ARTIFACTS = "factory_tracker_result_artifacts"
DELEGATIONS = "factory_triage_delegations"
HANDOFFS = "factory_triage_handoffs"
TRIAGE_TABLES = (HANDOFFS, DELEGATIONS, ARTIFACTS, RECEIPTS, DELIVERIES, GRANTS)
DROPPED_COLUMNS = {
    "execution_requests": {
        "completion_contract",
        "result_policy_digest",
        "model_session_reserved_at",
    },
    "agent_versions": {"result_policy", "result_policy_digest", "result_policy_schema_version"},
}
SHAPE_TABLES = (
    "execution_requests",
    "agent_versions",
    "factory_slack_posts",
    "factory_terminal_notices",
    *TRIAGE_TABLES,
)
KIND_CK = "factory_slack_posts_kind_ck"
LABEL_CK = "factory_terminal_notices_applied_label_ck"
RECOVERY_SHAPE_CK = "execution_requests_recovery_shape_ck"
TRIAGED = "curie-factory:triaged"
QUESTIONS = "curie-factory:questions"

CHANNEL = "C0EXAMPLE1"
IDENTITY = "acme-factory"
REPOSITORY = "acme-corp/acme-bot"
HEX64 = "a" * 64
REVISION_SHA = "c" * 40
TICKET_POLICY = {"results": ["findings", "ticket"], "implementation_label": "curie-implement"}
JSONB_COLUMNS = {"allowed_result_kinds", "output_labels", "content", "result_policy", "payload"}
OUTPUTS = (
    "output_tracker_kind",
    "output_tracker_host",
    "output_scope_id",
    "output_issue_id",
    "output_url",
    "output_body_sha256",
)

Statement = tuple[str, dict[str, Any]]


# --- seed helpers (ported from the 0162 and 0167 migration tests) ---------------------


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


def _agent(**values: Any) -> dict[str, Any]:
    agent_id = uuid.uuid4()
    return {"id": agent_id, "name": f"m0188-{agent_id.hex[:8]}", **values}


def _work_item(agent_id: uuid.UUID, issue: int) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "tracker_kind": "github",
        "tracker_host": "github.com",
        "tracker_scope_id": "101",
        "tracker_issue_id": str(issue),
        "code_host_kind": "github",
        "code_host_host": "github.com",
        "repository_project_id": "101",
        "code_host_installation_id": 202,
        "agent_id": agent_id,
        "repository_path": REPOSITORY,
        "conversation_id": f"acme-m0188-{issue}",
        "version": 1,
        "next_sequence": 2,
    }


def _queued(work_item_id: uuid.UUID, sequence: int, **values: Any) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "work_item_id": work_item_id,
        "sequence": sequence,
        "status": "queued",
        "version": 1,
        "execution_attempts": 0,
        **values,
    }


def _ended(
    work_item_id: uuid.UUID,
    sequence: int,
    *,
    status: str,
    cause: str,
    terminal_at: datetime,
    **values: Any,
) -> dict[str, Any]:
    """A started request that ended, in the state shape the lifecycle writes."""

    started = terminal_at - timedelta(minutes=10)
    return {
        **_queued(work_item_id, sequence),
        "status": status,
        "execution_attempts": 1,
        "wait_deadline": started + timedelta(minutes=30),
        "started_at": started,
        "execution_deadline": started + timedelta(hours=1),
        "terminal_at": terminal_at,
        "terminal_cause": cause,
        **values,
    }


def _tracker_result(request: dict[str, Any]) -> dict[str, Any]:
    """The 0167 columns a triage request carried."""

    return {
        **request,
        "completion_contract": "tracker_result",
        "result_policy_digest": HEX64,
        "model_session_reserved_at": request.get("started_at"),
    }


def _recovery(retried: dict[str, Any], sequence: int) -> dict[str, Any]:
    return _queued(
        retried["work_item_id"],
        sequence,
        execution_kind="publication_recovery",
        recovery_candidate_id=uuid.uuid4(),
        recovery_candidate_sha256="e" * 64,
        retry_of_request_id=retried["id"],
        retry_origin_request_id=retried["id"],
    )


def _grant(request: dict[str, Any], *, ticket: bool) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "work_item_id": request["work_item_id"],
        "execution_request_id": request["id"],
        "source_fingerprint": HEX64,
        "repository_id": "101",
        "allowed_result_kinds": ["findings", "ticket"] if ticket else ["findings"],
        "output_identity": f"execution-request:{request['id']}",
        "implementation_label": "curie-implement" if ticket else None,
        "expires_at": request["terminal_at"] + timedelta(hours=1),
        "source_content_sha256": "f" * 64,
    }


def _outputs(issue: int) -> dict[str, Any]:
    return {
        "output_tracker_kind": "github",
        "output_tracker_host": "github.com",
        "output_scope_id": "101",
        "output_issue_id": str(issue),
        "output_url": f"https://github.com/{REPOSITORY}/issues/{issue}",
        "output_body_sha256": "d" * 64,
    }


def _delivery(grant: dict[str, Any], *, state: str, ticket_issue: int | None) -> dict[str, Any]:
    submitted = grant["expires_at"] - timedelta(hours=1, minutes=5)
    row: dict[str, Any] = {
        "id": uuid.uuid4(),
        "grant_id": grant["id"],
        "execution_request_id": grant["execution_request_id"],
        "result_kind": "findings" if ticket_issue is None else "ticket",
        "reason": "not_reproduced" if ticket_issue is None else None,
        "proposal_sha256": "b" * 64,
        "runtime_epoch": 1,
        "state": state,
        "attempts": 0 if state == "owed" else 1,
        "submitted_at": submitted,
        "deadline_at": submitted + timedelta(seconds=900),
        "settled_at": None if state == "owed" else submitted + timedelta(seconds=60),
    }
    if ticket_issue is not None:
        row.update(_outputs(ticket_issue))
    return row


def _receipt(delivery: dict[str, Any], issue: int) -> dict[str, Any]:
    row = {
        "delivery_id": delivery["id"],
        "grant_id": delivery["grant_id"],
        "execution_request_id": delivery["execution_request_id"],
        "result_kind": delivery["result_kind"],
        "reason": delivery["reason"],
        "proposal_sha256": delivery["proposal_sha256"],
        "revision": REVISION_SHA,
        "result_comment_id": "9001",
        "result_comment_url": f"https://github.com/{REPOSITORY}/issues/{issue}#issuecomment-9001",
    }
    if delivery["result_kind"] == "ticket":
        row.update({name: delivery[name] for name in OUTPUTS}, output_labels=["curie-implement"])
    return row


def _artifact(delivery: dict[str, Any]) -> dict[str, Any]:
    return {
        "delivery_id": delivery["id"],
        "content": {"kind": delivery["result_kind"]},
        "expires_at": delivery["deadline_at"],
    }


def _delegation(request: dict[str, Any], issue: int) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "work_item_id": request["work_item_id"],
        "source_execution_request_id": request["id"],
        "actor_id": "4242",
        "actor_login": "acme-triager",
        "selection_event": "label",
        "tracker_kind": "github",
        "tracker_host": "github.com",
        "tracker_scope_id": "101",
        "tracker_issue_id": str(issue),
        "repository_id": "101",
        "result_policy_digest": HEX64,
        "target_label": "curie-implement",
        "installation_id": 202,
        "source_fingerprint": HEX64,
    }


def _handoff(delegation: dict[str, Any], ticket_issue: int) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "delegation_id": delegation["id"],
        "source_execution_request_id": delegation["source_execution_request_id"],
        **_outputs(ticket_issue),
        "next_attempt_at": datetime.now(UTC),
        "deadline_at": datetime.now(UTC) + timedelta(hours=1),
    }


def _thread(
    item: dict[str, Any], *, root_ts: str, card_ts: str | None = None, first_sequence: int = 1
) -> dict[str, Any]:
    return {
        "work_item_id": item["id"],
        "agent_id": item["agent_id"],
        "channel_identity": IDENTITY,
        "channel": CHANNEL,
        "root_ts": root_ts,
        "card_ts": card_ts,
        "first_sequence": first_sequence,
        "origin": "configured",
    }


def _root_post(item: dict[str, Any], root_ts: str) -> dict[str, Any]:
    issue = item["tracker_issue_id"]
    return {
        "work_item_id": item["id"],
        "post_key": "root",
        "kind": "root",
        "sequence": 0,
        "rank": 0,
        "ordinal": 0,
        "payload": {
            "repository_path": REPOSITORY,
            "issue_id": issue,
            "ticket_url": f"https://github.com/{REPOSITORY}/issues/{issue}",
        },
        "attempts": 1,
        "posted_at": datetime.now(UTC) - timedelta(hours=1),
        "posted_ref": root_ts,
    }


def _card_post(item: dict[str, Any], card_ts: str | None) -> dict[str, Any]:
    """The card's first post; posted when ``card_ts`` is given, else still owed."""

    return {
        "work_item_id": item["id"],
        "post_key": "card",
        "kind": "card",
        "sequence": 0,
        "rank": 0,
        "ordinal": 1,
        "payload": {"issue_id": item["tracker_issue_id"]},
        "attempts": 1 if card_ts else 0,
        "posted_at": datetime.now(UTC) - timedelta(hours=1) if card_ts else None,
        "posted_ref": card_ts,
    }


def _triage_post(request: dict[str, Any], *, posted: bool = True) -> dict[str, Any]:
    """The one result line a delivered triage request had, as its writer built it."""

    return {
        "work_item_id": request["work_item_id"],
        "post_key": f"triage:{request['id']}",
        "kind": "triage",
        "sequence": request["sequence"],
        "rank": 3,
        "ordinal": 0,
        "payload": {
            "result_kind": "findings",
            "reason": "not_reproduced",
            "revision": REVISION_SHA,
            "ticket_url": None,
            "result_comment_url": f"https://github.com/{REPOSITORY}/issues/1#issuecomment-9001",
            "handoff_admitted": None,
            "implementation_status_url": None,
            "refusal_reason": None,
        },
        "attempts": 1 if posted else 0,
        "posted_at": datetime.now(UTC) - timedelta(minutes=1) if posted else None,
        "posted_ref": "1800000000.999999" if posted else None,
    }


def _notice(request: dict[str, Any], label: str | None) -> dict[str, Any]:
    return {
        "execution_request_id": request["id"],
        "work_item_id": request["work_item_id"],
        "applied_label": label,
    }


def _bare_thread_rows(issue: int) -> tuple[dict[str, Any], dict[str, Any], list[Statement]]:
    """An agent, WorkItem, queued request and configured thread, valid at 0186 and at head."""

    agent = _agent()
    item = _work_item(agent["id"], issue)
    request = _queued(item["id"], 1)
    return (
        item,
        request,
        [
            _insert("agents", agent),
            _insert("work_items", item),
            _insert("execution_requests", request),
            _insert("factory_slack_threads", _thread(item, root_ts=f"1800000000.{issue:06d}")),
        ],
    )


# --- catalog helpers ------------------------------------------------------------------


def _tables() -> set[str]:
    rows = sql_rows("SELECT table_name FROM information_schema.tables WHERE table_schema = 'curie'")
    return {row[0] for row in rows}


def _shape() -> dict[str, dict[str, dict[str, Any]]]:
    """Columns (keyed by name, not position), constraint texts and index texts."""

    names = {"tables": list(SHAPE_TABLES)}
    shape: dict[str, dict[str, dict[str, Any]]] = {table: {} for table in SHAPE_TABLES}
    columns, constraints, indexes = _batch(
        (
            "SELECT table_name, column_name, data_type, character_maximum_length, "
            "is_nullable, column_default FROM information_schema.columns "
            "WHERE table_schema = 'curie' AND table_name = ANY(:tables)",
            names,
        ),
        (
            "SELECT t.relname AS table_name, c.conname, pg_get_constraintdef(c.oid) AS def "
            "FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid "
            "JOIN pg_namespace n ON n.oid = t.relnamespace "
            "WHERE n.nspname = 'curie' AND t.relname = ANY(:tables)",
            names,
        ),
        (
            "SELECT tablename AS table_name, indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = 'curie' AND tablename = ANY(:tables)",
            names,
        ),
    )
    for row in columns:
        shape[row["table_name"]][f"column:{row['column_name']}"] = (
            row["data_type"],
            row["character_maximum_length"],
            row["is_nullable"],
            row["column_default"],
        )
    for row in constraints:
        shape[row["table_name"]][f"constraint:{row['conname']}"] = row["def"]
    for row in indexes:
        shape[row["table_name"]][f"index:{row['indexname']}"] = row["indexdef"]
    return shape


def _check_values(name: str) -> set[str]:
    """The quoted literals of one CHECK, as pg_get_constraintdef prints them."""

    (row,) = sql_rows(
        "SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c "
        "JOIN pg_namespace n ON n.oid = c.connamespace "
        "WHERE n.nspname = 'curie' AND c.conname = :name",
        {"name": name},
    )
    return set(re.findall(r"'((?:[^']|'')*)'::text", row[0]))


def _head() -> str:
    head = ScriptDirectory.from_config(alembic_config()).get_current_head()
    assert head is not None
    return head


# --- AC3 -----------------------------------------------------------------------------


@dataclass(frozen=True)
class TriageWorld:
    version_id: uuid.UUID
    implementation: dict[str, Any]
    triage: dict[str, Any]
    recovery: dict[str, Any]
    triage_item: dict[str, Any]


def _seed_triage_world() -> TriageWorld:
    """At 0186: every kind of row the triage path wrote, beside kept rows."""

    now = datetime.now(UTC)
    agent = _agent()
    built = _work_item(agent["id"], 6101)
    triaged = _work_item(agent["id"], 6102)
    implementation = _ended(built["id"], 1, status="completed", cause="completed", terminal_at=now)
    recovery = _recovery(implementation, 2)
    triage = _tracker_result(
        _ended(triaged["id"], 1, status="completed", cause="completed", terminal_at=now)
    )
    grant = _grant(triage, ticket=True)
    delivery = _delivery(grant, state="delivered", ticket_issue=6201)
    delegation = _delegation(triage, 6102)
    version = {
        "id": uuid.uuid4(),
        "agent_id": agent["id"],
        "version_label": "v1",
        "created_by": "m0188",
        "result_policy_schema_version": 1,
        "result_policy_digest": HEX64,
        "result_policy": TICKET_POLICY,
    }
    root_ts = "1800000000.006102"
    _batch(
        _insert("agents", agent),
        _insert("agent_versions", version),
        _insert("work_items", built),
        _insert("work_items", triaged),
        _insert("execution_requests", implementation),
        _insert("execution_requests", recovery),
        _insert("execution_requests", triage),
        _insert(GRANTS, grant),
        _insert(DELIVERIES, delivery),
        _insert(RECEIPTS, _receipt(delivery, 6102)),
        _insert(ARTIFACTS, _artifact(delivery)),
        _insert(DELEGATIONS, delegation),
        _insert(HANDOFFS, _handoff(delegation, 6201)),
        _insert("factory_terminal_notices", _notice(triage, TRIAGED)),
        _insert("factory_terminal_notices", _notice(implementation, "curie-factory:pr-open")),
        _insert("factory_slack_threads", _thread(triaged, root_ts=root_ts)),
        _insert("factory_slack_posts", _root_post(triaged, root_ts)),
        _insert("factory_slack_posts", _triage_post(triage)),
    )
    return TriageWorld(version["id"], implementation, triage, recovery, triaged)


def test_upgrade_with_triage_rows_drops_the_triage_schema_and_keeps_kept_rows(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    world = _seed_triage_world()
    assert set(TRIAGE_TABLES) <= _tables()
    for table in TRIAGE_TABLES:
        assert sql_rows(f"SELECT count(*) FROM curie.{table}") == [(1,)], table

    command.upgrade(alembic_config(), _head())

    assert stamped_revision() == _head()
    assert not set(TRIAGE_TABLES) & _tables()
    for table, columns in DROPPED_COLUMNS.items():
        assert not columns & column_names(table), table
    requests = {
        row["id"]: row["execution_kind"]
        for row in sql_dicts("SELECT id, execution_kind FROM curie.execution_requests")
    }
    assert requests == {
        world.implementation["id"]: "implementation",
        world.triage["id"]: "implementation",
        world.recovery["id"]: "publication_recovery",
    }
    assert sql_rows("SELECT id FROM curie.agent_versions") == [(world.version_id,)]
    labels = {
        row["execution_request_id"]: row["applied_label"]
        for row in sql_dicts(
            "SELECT execution_request_id, applied_label FROM curie.factory_terminal_notices"
        )
    }
    assert labels == {
        world.triage["id"]: None,
        world.implementation["id"]: "curie-factory:pr-open",
    }
    posts = sql_dicts("SELECT post_key, kind FROM curie.factory_slack_posts ORDER BY id")
    assert posts == [{"post_key": "root", "kind": "root"}]


def test_publication_recovery_shape_check_still_fires(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    item, request, rows = _bare_thread_rows(6301)
    _batch(*rows)
    orphan = {
        **_recovery(request, 2),
        "retry_of_request_id": None,
        "retry_origin_request_id": None,
    }
    with pytest.raises(IntegrityError, match=RECOVERY_SHAPE_CK):
        sql_rows(*_insert("execution_requests", orphan))
    # The same request naming what it retries is accepted.
    _batch(_insert("execution_requests", _recovery(request, 2)))
    assert sql_rows(
        "SELECT count(*) FROM curie.execution_requests WHERE work_item_id = :id",
        {"id": item["id"]},
    ) == [(2,)]


def test_old_label_and_post_kind_are_refused(isolated_migration_db: IsolatedMigrationDb) -> None:
    isolated_migration_db.at(REVISION)
    _item, request, rows = _bare_thread_rows(6401)
    _batch(*rows)
    with pytest.raises(IntegrityError, match=KIND_CK):
        sql_rows(*_insert("factory_slack_posts", _triage_post(request, posted=False)))
    with pytest.raises(IntegrityError, match=LABEL_CK):
        sql_rows(*_insert("factory_terminal_notices", _notice(request, TRIAGED)))


def test_label_and_kind_checks_lose_only_the_triage_values(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(BELOW)
    kinds_before, labels_before = _check_values(KIND_CK), _check_values(LABEL_CK)
    assert "triage" in kinds_before
    assert {TRIAGED, QUESTIONS, ""} <= labels_before

    command.upgrade(alembic_config(), REVISION)

    assert _check_values(KIND_CK) == kinds_before - {"triage"}
    assert _check_values(LABEL_CK) == labels_before - {TRIAGED}


def test_the_questions_label_is_still_accepted(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at(REVISION)
    _item, request, rows = _bare_thread_rows(6501)
    _batch(
        *rows,
        _insert("factory_terminal_notices", _notice(request, QUESTIONS)),
        _insert(
            "factory_slack_posts",
            {
                "work_item_id": request["work_item_id"],
                "post_key": f"needs_person:{request['id']}",
                "kind": "needs_person",
                "sequence": 1,
                "rank": 3,
                "ordinal": 0,
                "payload": {"cause": "questions_asked"},
            },
        ),
    )
    assert sql_rows("SELECT applied_label FROM curie.factory_terminal_notices") == [(QUESTIONS,)]
    assert sql_rows("SELECT kind FROM curie.factory_slack_posts") == [("needs_person",)]


def test_downgrade_restores_the_previous_shape_empty_and_upgrades_again(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    config = alembic_config()
    isolated_migration_db.at(BELOW)
    before = _shape()
    assert all(before.values()), before
    world = _seed_triage_world()

    command.upgrade(config, REVISION)
    assert _shape() != before
    command.downgrade(config, BELOW)

    assert stamped_revision() == BELOW
    assert _shape() == before
    for table in TRIAGE_TABLES:
        assert sql_rows(f"SELECT count(*) FROM curie.{table}") == [(0,)], table
    contracts = {
        row["id"]: row["completion_contract"]
        for row in sql_dicts("SELECT id, completion_contract FROM curie.execution_requests")
    }
    assert contracts == dict.fromkeys(
        (world.implementation["id"], world.triage["id"], world.recovery["id"]),
        "implementation",
    )
    assert sql_rows(
        "SELECT result_policy, result_policy_digest, result_policy_schema_version "
        "FROM curie.agent_versions"
    ) == [(None, None, None)]

    command.upgrade(config, REVISION)
    assert stamped_revision() == REVISION
    assert not set(TRIAGE_TABLES) & _tables()


def test_the_drop_requires_forward_only() -> None:
    # A later contract revision may raise the floor again, never below this one.
    assert load_window().schema_min >= REVISION
    kinds = load_kinds()
    assert kinds[REVISION] == KIND_CONTRACT
    window = AppWindow(schema_min=REVISION, schema_head=REVISION)

    refused = plan_upgrade(
        current_revision=BELOW,
        window=window,
        kinds=kinds,
        pending=(REVISION,),
        forward_only=False,
    )
    assert refused.action == "refuse"
    assert refused.rollback_compatible is False
    assert refused.pending[0].kind == KIND_CONTRACT

    allowed = plan_upgrade(
        current_revision=BELOW,
        window=window,
        kinds=kinds,
        pending=(REVISION,),
        forward_only=True,
    )
    assert allowed.action == "apply"
    assert allowed.rollback_compatible is False
    assert allowed.pending[0].kind == KIND_CONTRACT


# --- AC14: the Slack progress lane after the drop ------------------------------------


@dataclass
class RecordingSlack:
    """The reply sink the progress loop sends through; records every call."""

    calls: list[tuple[ReplyEvent, TargetRoute]] = field(default_factory=list)

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: TargetRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        del best_effort_unreachable
        self.calls.append((event, route))
        return ReplyAck(ref=f"1800000100.{len(self.calls):06d}")


def _progress_pass(slack: RecordingSlack) -> None:
    """One pass of the production progress loop over the real SQL store."""

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        try:
            loop = FactorySlackProgressLoop(
                store=PostgresFactorySlackStore(
                    engine,
                    schema="curie",
                    lease_owner="m0188-progress",
                    console_origin="https://console.example.com",
                    # The liveness leg below edits a card the upgrade stamped a
                    # moment earlier; the edit spacing is not under test.
                    card_edit_spacing_seconds=0.001,
                ),
                replies=slack,
                interval_seconds=1.0,
                batch_limit=50,
            )
            await loop.run_pass()
        finally:
            await engine.dispose()

    asyncio.run(run())


@dataclass(frozen=True)
class QuietThread:
    item: dict[str, Any]
    root_ts: str
    card_ts: str | None


def _quiet_thread(
    agent_id: uuid.UUID,
    issue: int,
    *,
    card: bool,
    status: str,
    cause: str,
    delivery_state: str,
    terminal_at: datetime,
    owed_card: bool = False,
    owed_needs_person: bool = False,
) -> tuple[QuietThread, list[Statement]]:
    """A configured thread whose only request is a delivered or owed triage request."""

    item = _work_item(agent_id, issue)
    root_ts = f"1800000000.{issue:06d}"
    card_ts = f"1800000001.{issue:06d}" if card else None
    request = _tracker_result(
        _ended(item["id"], 1, status=status, cause=cause, terminal_at=terminal_at)
    )
    grant = _grant(request, ticket=False)
    delivery = _delivery(grant, state=delivery_state, ticket_issue=None)
    rows = [
        _insert("work_items", item),
        _insert("execution_requests", request),
        _insert(GRANTS, grant),
        _insert(DELIVERIES, delivery),
        _insert("factory_slack_threads", _thread(item, root_ts=root_ts, card_ts=card_ts)),
        _insert("factory_slack_posts", _root_post(item, root_ts)),
        _insert("factory_slack_posts", _triage_post(request)),
    ]
    if delivery_state == "delivered":
        rows.append(_insert(RECEIPTS, _receipt(delivery, issue)))
    if card:
        rows.append(_insert("factory_slack_posts", _card_post(item, card_ts)))
    if owed_card:
        # Written by a worker that already ignored the contract, not yet sent.
        rows.append(_insert("factory_slack_posts", _card_post(item, None)))
    if owed_needs_person:
        rows.append(
            _insert(
                "factory_slack_posts",
                {
                    "work_item_id": item["id"],
                    "post_key": f"needs_person:{request['id']}",
                    "kind": "needs_person",
                    "sequence": 1,
                    "rank": 3,
                    "ordinal": 0,
                    "payload": {"cause": cause},
                },
            )
        )
    return QuietThread(item, root_ts, card_ts), rows


def _thread_of(event: ReplyEvent) -> str | None:
    assert isinstance(event, ReplyPost | ReplyUpdate), event
    return event.target.conversation_id


def test_retained_triage_requests_stay_quiet_in_the_slack_progress_lane(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """AC14. Before 0188 the worker told triage requests apart by their contract;
    after it nothing does. Seeded at 0186 on configured threads, through the
    production loop and its SQL store:

    1. a completed triage request whose findings were posted, on a thread with
       a card (no edit owed) and on one with none (no card post owed, and an
       unposted card post a newer worker wrote is withdrawn);
    2. a request that ended ``tracker_result_undelivered`` whose findings were
       delivered (or still owed) later, with and without an unposted
       needs a person reply already written.

    Without the upgrade's first step this pass posts a card, edits two and
    replies "needs you" twice. A new implementation request on two of those
    WorkItems still gets its card post and its card edit.
    """

    isolated_migration_db.at(BELOW)
    agent = _agent(
        factory_slack_channel=CHANNEL, factory_slack_since=datetime(2026, 1, 1, tzinfo=UTC)
    )
    now = datetime.now(UTC)
    edited, edited_rows = _quiet_thread(
        agent["id"],
        701,
        card=True,
        status="completed",
        cause="completed",
        delivery_state="delivered",
        terminal_at=now,
    )
    uncarded, uncarded_rows = _quiet_thread(
        agent["id"],
        702,
        card=False,
        status="completed",
        cause="completed",
        delivery_state="delivered",
        terminal_at=now,
        owed_card=True,
    )
    announced, announced_rows = _quiet_thread(
        agent["id"],
        703,
        card=True,
        status="failed",
        cause="tracker_result_undelivered",
        delivery_state="delivered",
        terminal_at=now,
    )
    owed, owed_rows = _quiet_thread(
        agent["id"],
        704,
        card=True,
        status="failed",
        cause="tracker_result_undelivered",
        delivery_state="owed",
        terminal_at=now,
        owed_needs_person=True,
    )
    _batch(
        _insert("agents", agent),
        _insert(
            "agent_channels",
            {
                "id": uuid.uuid4(),
                "agent_id": agent["id"],
                "kind": "slack",
                "address": CHANNEL,
                "adapter": IDENTITY,
            },
        ),
        *edited_rows,
        *uncarded_rows,
        *announced_rows,
        *owed_rows,
    )
    # The states the reproduction needs: every run ended after its thread's
    # card cutoff, and no card has been edited since.
    cutoffs = sql_dicts(
        "SELECT t.card_upgrade_cutoff, t.card_edited_at, q.terminal_at "
        "FROM curie.factory_slack_threads t JOIN curie.execution_requests q "
        "ON q.work_item_id = t.work_item_id"
    )
    assert len(cutoffs) == 4
    assert all(
        row["terminal_at"] >= row["card_upgrade_cutoff"] and row["card_edited_at"] is None
        for row in cutoffs
    ), cutoffs

    command.upgrade(alembic_config(), REVISION)
    slack = RecordingSlack()
    _progress_pass(slack)
    assert slack.calls == []
    retired = sql_dicts(
        "SELECT work_item_id, failed_at IS NOT NULL AS failed, last_error "
        "FROM curie.factory_slack_posts WHERE kind = 'needs_person' ORDER BY work_item_id"
    )
    assert sorted(retired, key=lambda row: str(row["work_item_id"])) == sorted(
        (
            {
                "work_item_id": thread.item["id"],
                "failed": True,
                "last_error": "retired_triage_request",
            }
            for thread in (announced, owed)
        ),
        key=lambda row: str(row["work_item_id"]),
    )

    # Liveness: the quieting is per retained request, not per thread.
    _batch(
        _insert("execution_requests", _queued(edited.item["id"], 2)),
        _insert("execution_requests", _queued(uncarded.item["id"], 2)),
    )
    _progress_pass(slack)
    touched = {_thread_of(event) for event, _ in slack.calls}
    assert touched == {edited.root_ts, uncarded.root_ts}, slack.calls
    assert all(route.adapter == IDENTITY for _, route in slack.calls)
    edits = [event for event, _ in slack.calls if isinstance(event, ReplyUpdate)]
    assert edited.card_ts in {event.target.reply_ref for event in edits}
    posts = [event for event, _ in slack.calls if isinstance(event, ReplyPost)]
    assert [(post.target.conversation_id, post.delivery_id) for post in posts] == [
        (uncarded.root_ts, delivery_id_for(uncarded.item["id"], "card"))
    ]


def test_an_undiscovered_triage_work_item_stays_quiet_until_its_next_request(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    """Correction 14 through first discovery. Seeded at 0186 with the agent's
    Slack destination configured and no thread row yet: a completed triage
    request whose findings were delivered, and one that ended
    ``tracker_result_undelivered`` with its findings still owed (and a third
    completed one). Without the upgrade's third step this pass discovers them
    and posts their roots, a card for each and a needs a person reply. With it
    the pass is silent, and a new request on one WorkItem gets its root and its
    card. A request that ended while the destination was cleared is behind the
    restarted cutoff, so it does not root a recorded thread, and the next
    eligible request does. The downgrade deletes a recorded thread that was
    never rooted, so the 0186 worker can discover its WorkItem again.
    """

    isolated_migration_db.at(BELOW)
    agent = _agent(
        factory_slack_channel=CHANNEL, factory_slack_since=datetime(2026, 1, 1, tzinfo=UTC)
    )
    now = datetime.now(UTC)
    rows: list[Statement] = [
        _insert("agents", agent),
        _insert(
            "agent_channels",
            {
                "id": uuid.uuid4(),
                "agent_id": agent["id"],
                "kind": "slack",
                "address": CHANNEL,
                "adapter": IDENTITY,
            },
        ),
    ]
    items = []
    for issue, status, cause, state in (
        (711, "completed", "completed", "delivered"),
        (712, "failed", "tracker_result_undelivered", "owed"),
        (713, "completed", "completed", "delivered"),
    ):
        item = _work_item(agent["id"], issue)
        request = _tracker_result(
            _ended(item["id"], 1, status=status, cause=cause, terminal_at=now)
        )
        grant = _grant(request, ticket=False)
        delivery = _delivery(grant, state=state, ticket_issue=None)
        rows += [
            _insert("work_items", item),
            _insert("execution_requests", request),
            _insert(GRANTS, grant),
            _insert(DELIVERIES, delivery),
        ]
        if state == "delivered":
            rows.append(_insert(RECEIPTS, _receipt(delivery, issue)))
        items.append(item)
    _batch(*rows)
    assert sql_rows("SELECT count(*) FROM curie.factory_slack_threads") == [(0,)]

    command.upgrade(alembic_config(), REVISION)
    slack = RecordingSlack()
    _progress_pass(slack)
    assert slack.calls == []
    threads = sql_dicts(
        "SELECT work_item_id, first_sequence, root_ts, channel_identity, channel "
        "FROM curie.factory_slack_threads"
    )
    assert sorted(threads, key=lambda row: str(row["work_item_id"])) == sorted(
        (
            {
                "work_item_id": item["id"],
                "first_sequence": 2,
                "root_ts": None,
                "channel_identity": IDENTITY,
                "channel": CHANNEL,
            }
            for item in items
        ),
        key=lambda row: str(row["work_item_id"]),
    )

    # Liveness: the next request on the WorkItem gets the thread a newly
    # discovered one would get, and the other WorkItem stays silent.
    woken = items[0]
    _batch(
        _insert("execution_requests", _queued(woken["id"], 2)),
        ("UPDATE curie.work_items SET next_sequence = 3 WHERE id = :id", {"id": woken["id"]}),
    )
    _progress_pass(slack)
    posts = [event for event, _ in slack.calls if isinstance(event, ReplyPost)]
    assert [post.delivery_id for post in posts] == [
        delivery_id_for(woken["id"], "root"),
        delivery_id_for(woken["id"], "card"),
    ], slack.calls
    assert all(route.adapter == IDENTITY for _, route in slack.calls)

    # A request that ended while the destination was cleared is before the
    # restarted cutoff: it does not root the recorded thread, as it would not
    # make an ordinary WorkItem's thread.
    late = items[1]
    _batch(
        (
            "UPDATE curie.agents SET factory_slack_channel = NULL, factory_slack_since = NULL "
            "WHERE id = :id",
            {"id": agent["id"]},
        ),
    )
    _batch(
        _insert(
            "execution_requests",
            _ended(
                late["id"],
                2,
                status="completed",
                cause="completed",
                terminal_at=datetime.now(UTC) - timedelta(seconds=1),
            ),
        ),
        ("UPDATE curie.work_items SET next_sequence = 3 WHERE id = :id", {"id": late["id"]}),
    )
    _batch(
        (
            "UPDATE curie.agents SET factory_slack_channel = :channel, "
            "factory_slack_since = now() WHERE id = :id",
            {"id": agent["id"], "channel": CHANNEL},
        ),
    )
    before = len(slack.calls)
    _progress_pass(slack)
    assert slack.calls[before:] == []

    # The next eligible request roots it, from that request.
    _batch(
        _insert("execution_requests", _queued(late["id"], 3)),
        ("UPDATE curie.work_items SET next_sequence = 4 WHERE id = :id", {"id": late["id"]}),
    )
    _progress_pass(slack)
    later = [event for event, _ in slack.calls[before:] if isinstance(event, ReplyPost)]
    assert [post.delivery_id for post in later] == [
        delivery_id_for(late["id"], "root"),
        delivery_id_for(late["id"], "card"),
    ], slack.calls[before:]
    assert sql_rows(
        "SELECT first_sequence FROM curie.factory_slack_threads WHERE work_item_id = :id",
        {"id": late["id"]},
    ) == [(3,)]

    command.downgrade(alembic_config(), BELOW)
    kept = {row[0] for row in sql_rows("SELECT work_item_id FROM curie.factory_slack_threads")}
    assert kept == {items[0]["id"], items[1]["id"]}

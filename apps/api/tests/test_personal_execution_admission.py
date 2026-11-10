"""Trusted personal admission binds an immutable owner tuple (#5376 AC1).

A WorkItem and every ExecutionRequest carry ``tenant_id``, ``model_auth_mode``
(``service`` or ``personal``), ``owner_principal_id`` and
``enrollment_generation``. Only the internal worker-token admission route can
name an owner, as ``owner: {tenant_id, principal_id, enrollment_generation}``
beside ``model_auth_mode``; it is validated against the admitted agent's
tenant, the principal's status and the principal's
``personal_claude_enrollments`` row in the transaction that inserts the
WorkItem. Every refusal is HTTP 409 ``{"detail": {"code": ...}}`` and leaves no
WorkItem and no ExecutionRequest behind; each one is paired with the valid
admission that passes in the same setup.

Both tracker ingress adapters build their facts from the installation setting
``FACTORY_MODEL_AUTH_MODE`` and never from requester text. With ``personal``
they refuse ``personal_owner_required`` before any row exists, even when the
sender login and the ticket text name a real, enrolled principal; with the
default they admit a service WorkItem.

Postgres and Valkey are real. Only forge HTTP is faked, through the existing
GitHub and Jira fakes. Tenants, principals and enrollments are seeded with raw
SQL and removed by ``owned_rows``. Placeholder identifiers only.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import uuid
from collections.abc import Awaitable, Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.forges.identity import notice_request_id
from curie_api.forges.types import Disposition, MarkedNotice
from curie_api.main import create_app
from fastapi.testclient import TestClient
from forge_fakes import jira as jira_fake
from forge_fakes.github import (
    LABEL,
    REPO,
    REPO_ID,
    SENDER,
    GitHubAPI,
    _Credentials,
    _issue_event,
    _post,
    _sender,
    factory_env,
    fake_installation_tokens,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine
from sqlalchemy.pool import NullPool
from test_factory_tracker_intake import (  # noqa: F401  (make_intake is a fixture)
    STARTER_ID,
    Intake,
    _document,
    make_intake,
)

MARK = "test-personal-admission-"
DEFAULT_TENANT = UUID("00000000-0000-0000-0000-000000000001")
WORKER_TOKEN = "personal-admission-worker-token"
WORKER_HEADERS = {"X-Curie-Worker-Token": WORKER_TOKEN}
ADMISSIONS = "/v1/internal/work-items/admissions"
ADDRESS = "C0EXAMPLE1"
OTHER_ADDRESS = "C0EXAMPLE2"
WIRE_CONVERSATION = "1700000000.000100"
SCOPE = "101"
OBJECTIVE = "Implement the admitted work item"
REQUESTER = "U0REQUEST1"
OWNER_NAME = "Acme Owner"
OWNER_GENERATION = 3
PEER_GENERATION = 1
FOREIGN_GENERATION = 2
_ISSUES = itertools.count(53760)
_GITHUB_ISSUES = itertools.count(53900)


# --- database access -------------------------------------------------------------------


def _run[T](body: Callable[[AsyncConnection], Awaitable[T]]) -> T:
    """Run ``body`` in one committed transaction on a fresh engine."""

    async def go() -> T:
        engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
        try:
            async with engine.begin() as connection:
                return await body(connection)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _rows(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def body(connection: AsyncConnection) -> list[dict[str, Any]]:
        result = await connection.execute(text(statement), params or {})
        return [dict(row) for row in result.mappings()] if result.returns_rows else []

    return _run(body)


def _count(statement: str, params: dict[str, Any] | None = None) -> int:
    [row] = _rows(statement, params)
    return int(row["n"])


async def _exec(connection: AsyncConnection, statement: str, params: dict[str, Any]) -> None:
    await connection.execute(text(statement), params)


# --- seeding ---------------------------------------------------------------------------


async def _tenant(connection: AsyncConnection) -> UUID:
    tenant_id = uuid.uuid4()
    await _exec(
        connection,
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :d, 'active')",
        {"id": tenant_id, "d": f"{MARK}{tenant_id}"},
    )
    return tenant_id


async def _principal(
    connection: AsyncConnection,
    tenant_id: UUID,
    *,
    status: str = "active",
    display_name: str = OWNER_NAME,
) -> UUID:
    principal_id = uuid.uuid4()
    await _exec(
        connection,
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status, display_name) "
        "VALUES (:id, :tenant, :subject, 'human', :status, :name)",
        {
            "id": principal_id,
            "tenant": tenant_id,
            "subject": f"{MARK}{principal_id}",
            "status": status,
            "name": display_name,
        },
    )
    return principal_id


async def _enroll(
    connection: AsyncConnection,
    tenant_id: UUID,
    principal_id: UUID,
    *,
    generation: int,
    active: bool = True,
) -> None:
    await _exec(
        connection,
        "INSERT INTO curie.personal_claude_enrollments "
        "(id, tenant_id, principal_id, generation, active, version) "
        "VALUES (:id, :tenant, :principal, :generation, :active, 1)",
        {
            "id": uuid.uuid4(),
            "tenant": tenant_id,
            "principal": principal_id,
            "generation": generation,
            "active": active,
        },
    )


async def _agent(connection: AsyncConnection, tenant_id: UUID, address: str) -> UUID:
    """An agent in ``tenant_id`` with one Slack binding in the same tenant."""

    agent_id = uuid.uuid4()
    await _exec(
        connection,
        "INSERT INTO curie.agents (id, tenant_id, name, repo_full_name) "
        "VALUES (:id, :tenant, :name, :repo)",
        {
            "id": agent_id,
            "tenant": tenant_id,
            "name": f"acme-bot-{agent_id.hex[:8]}",
            "repo": REPO,
        },
    )
    await _exec(
        connection,
        "INSERT INTO curie.agent_channels (id, agent_id, kind, address, adapter, tenant_id) "
        "VALUES (:id, :agent, 'slack', :address, 'default', :tenant)",
        {"id": uuid.uuid4(), "agent": agent_id, "address": address, "tenant": tenant_id},
    )
    return agent_id


def _owner(tenant_id: UUID, principal_id: UUID, generation: int) -> dict[str, Any]:
    return {
        "tenant_id": str(tenant_id),
        "principal_id": str(principal_id),
        "enrollment_generation": generation,
    }


@dataclass(frozen=True)
class World:
    """Two tenants. The agent and every principal but ``foreign`` live in ``tenant``."""

    tenant: UUID
    agent: UUID
    owner: UUID  # active, enrolled at OWNER_GENERATION
    peer: UUID  # active, enrolled at PEER_GENERATION
    disabled: UUID  # disabled, active enrollment
    revoked: UUID  # revoked, active enrollment
    unenrolled: UUID  # active, no enrollment row
    lapsed: UUID  # active, inactive enrollment
    other_tenant: UUID
    other_agent: UUID
    foreign: UUID  # active in other_tenant, enrolled at FOREIGN_GENERATION

    def owner_context(self, generation: int = OWNER_GENERATION) -> dict[str, Any]:
        return _owner(self.tenant, self.owner, generation)

    def foreign_context(self) -> dict[str, Any]:
        return _owner(self.other_tenant, self.foreign, FOREIGN_GENERATION)


async def _seed_world(connection: AsyncConnection) -> World:
    tenant = await _tenant(connection)
    other_tenant = await _tenant(connection)
    agent = await _agent(connection, tenant, ADDRESS)
    other_agent = await _agent(connection, other_tenant, OTHER_ADDRESS)
    owner = await _principal(connection, tenant)
    await _enroll(connection, tenant, owner, generation=OWNER_GENERATION)
    peer = await _principal(connection, tenant, display_name="Acme Peer")
    await _enroll(connection, tenant, peer, generation=PEER_GENERATION)
    disabled = await _principal(connection, tenant, status="disabled")
    await _enroll(connection, tenant, disabled, generation=1)
    revoked = await _principal(connection, tenant, status="revoked")
    await _enroll(connection, tenant, revoked, generation=1)
    unenrolled = await _principal(connection, tenant)
    lapsed = await _principal(connection, tenant)
    await _enroll(connection, tenant, lapsed, generation=1, active=False)
    foreign = await _principal(connection, other_tenant)
    await _enroll(connection, other_tenant, foreign, generation=FOREIGN_GENERATION)
    return World(
        tenant=tenant,
        agent=agent,
        owner=owner,
        peer=peer,
        disabled=disabled,
        revoked=revoked,
        unenrolled=unenrolled,
        lapsed=lapsed,
        other_tenant=other_tenant,
        other_agent=other_agent,
        foreign=foreign,
    )


def _delete_owned() -> tuple[int, int, int]:
    """Remove every row this module seeded; return what survived."""

    async def body(connection: AsyncConnection) -> tuple[int, int, int]:
        tenants = "SELECT id FROM curie.tenants WHERE deployment_id LIKE :mark"
        principals = "SELECT id FROM curie.principals WHERE idp_subject LIKE :mark"
        mark = {"mark": f"{MARK}%"}
        await _exec(connection, f"DELETE FROM curie.agents WHERE tenant_id IN ({tenants})", mark)
        await _exec(
            connection,
            f"DELETE FROM curie.work_items WHERE owner_principal_id IN ({principals})",
            mark,
        )
        await _exec(
            connection,
            "DELETE FROM curie.personal_claude_enrollments "
            f"WHERE principal_id IN ({principals}) OR tenant_id IN ({tenants})",
            mark,
        )
        await _exec(
            connection,
            "DELETE FROM curie.principals "
            f"WHERE idp_subject LIKE :mark OR tenant_id IN ({tenants})",
            mark,
        )
        await _exec(connection, "DELETE FROM curie.tenants WHERE deployment_id LIKE :mark", mark)
        counts = []
        for statement in (
            f"SELECT count(*) FROM ({tenants}) AS owned",
            f"SELECT count(*) FROM ({principals}) AS owned",
            "SELECT count(*) FROM curie.personal_claude_enrollments "
            f"WHERE principal_id IN ({principals}) OR tenant_id IN ({tenants})",
        ):
            counts.append(int((await connection.execute(text(statement), mark)).scalar_one()))
        return counts[0], counts[1], counts[2]

    return _run(body)


@pytest.fixture
def owned_rows(clean_db: None) -> Iterator[None]:
    assert _delete_owned() == (0, 0, 0)
    yield
    assert _delete_owned() == (0, 0, 0), "a seeded tenant, principal or enrollment survived"


@pytest.fixture
def world(owned_rows: None) -> World:
    return _run(_seed_world)


@pytest.fixture
def dispatch_client(clean_db: None, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    monkeypatch.delenv("FACTORY_MODEL_AUTH_MODE", raising=False)
    get_settings.cache_clear()
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


# --- the internal admission route ------------------------------------------------------


def _body(
    agent_id: UUID,
    address: str,
    number: int,
    request_id: UUID,
    *,
    mode: str | None = None,
    owner: dict[str, Any] | None = None,
    requester: str = REQUESTER,
    objective: str = OBJECTIVE,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "agent_id": str(agent_id),
        "kind": "slack",
        "address": address,
        "reply_conversation_id": WIRE_CONVERSATION,
        "tracker": {
            "kind": "github",
            "host": "github.com",
            "scope_id": SCOPE,
            "issue_id": str(number),
        },
        "repository": {
            "code_host_kind": "github",
            "host": "github.com",
            "project_id": SCOPE,
            "path": REPO,
        },
        "code_host_installation_id": 202,
        "objective": objective,
        "requester": requester,
        "request_id": str(request_id),
    }
    if mode is not None:
        body["model_auth_mode"] = mode
    if owner is not None:
        body["owner"] = owner
    return body


def _admit(client: TestClient, body: dict[str, Any]) -> httpx.Response:
    return client.post(ADMISSIONS, json=body, headers=WORKER_HEADERS)


def _assert_refused(response: httpx.Response, code: str) -> None:
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": {"code": code}}


_TUPLE = "tenant_id, model_auth_mode, owner_principal_id, enrollment_generation"


def _work_items(number: int) -> list[dict[str, Any]]:
    return _rows(
        f"SELECT id, {_TUPLE} FROM curie.work_items WHERE tracker_kind = 'github' "
        "AND tracker_host = 'github.com' AND tracker_scope_id = :scope "
        "AND tracker_issue_id = :number",
        {"scope": SCOPE, "number": str(number)},
    )


def _requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.requester, r.tenant_id, r.model_auth_mode, r.owner_principal_id, "
        "r.enrollment_generation FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.tracker_kind = 'github' AND w.tracker_host = 'github.com' "
        "AND w.tracker_scope_id = :scope AND w.tracker_issue_id = :number "
        "ORDER BY r.sequence",
        {"scope": SCOPE, "number": str(number)},
    )


def _request_exists(request_id: UUID) -> bool:
    return (
        _count(
            "SELECT count(*) AS n FROM curie.execution_requests WHERE id = :id",
            {"id": request_id},
        )
        > 0
    )


def _tuple(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["tenant_id"],
        row["model_auth_mode"],
        row["owner_principal_id"],
        row["enrollment_generation"],
    )


def _assert_no_runnable_state(number: int, request_id: UUID) -> None:
    assert _work_items(number) == []
    assert _requests(number) == []
    assert not _request_exists(request_id)


def _assert_views(
    body: dict[str, Any],
    *,
    tenant: UUID,
    mode: str,
    owner: UUID | None,
    generation: int | None,
) -> None:
    for view in (body["work_item"], body["request"]):
        assert view["tenant_id"] == str(tenant), view
        assert view["model_auth_mode"] == mode, view
        assert view["owner_principal_id"] == (None if owner is None else str(owner)), view
        assert view["enrollment_generation"] == generation, view


def _assert_one_bound(
    number: int,
    request_id: UUID,
    *,
    tenant: UUID,
    mode: str,
    owner: UUID | None,
    generation: int | None,
) -> None:
    expected = (tenant, mode, owner, generation)
    [item] = _work_items(number)
    assert _tuple(item) == expected
    [request] = _requests(number)
    assert request["id"] == request_id
    assert _tuple(request) == expected


def test_personal_admission_binds_the_owner_tuple_on_the_work_item_and_first_request(
    dispatch_client: TestClient, world: World
) -> None:
    number = next(_ISSUES)
    request_id = uuid.uuid4()
    # The requester text names another enrolled principal; it is only text.
    requester = f"slack:{world.peer}:Acme Peer"

    admitted = _admit(
        dispatch_client,
        _body(
            world.agent,
            ADDRESS,
            number,
            request_id,
            mode="personal",
            owner=world.owner_context(),
            requester=requester,
        ),
    )

    assert admitted.status_code == 200, admitted.text
    body = admitted.json()
    assert body["replayed"] is False
    assert body["request"]["id"] == str(request_id)
    _assert_views(
        body, tenant=world.tenant, mode="personal", owner=world.owner, generation=OWNER_GENERATION
    )
    _assert_one_bound(
        number,
        request_id,
        tenant=world.tenant,
        mode="personal",
        owner=world.owner,
        generation=OWNER_GENERATION,
    )
    [request] = _requests(number)
    assert request["requester"] == requester


@pytest.mark.parametrize("mode", [None, "service"], ids=["default", "explicit"])
def test_service_admission_stays_live_with_no_subscription_owner(
    dispatch_client: TestClient, world: World, mode: str | None
) -> None:
    number = next(_ISSUES)
    request_id = uuid.uuid4()

    admitted = _admit(dispatch_client, _body(world.agent, ADDRESS, number, request_id, mode=mode))

    assert admitted.status_code == 200, admitted.text
    _assert_views(admitted.json(), tenant=world.tenant, mode="service", owner=None, generation=None)
    _assert_one_bound(
        number, request_id, tenant=world.tenant, mode="service", owner=None, generation=None
    )


def test_enrollment_presence_does_not_make_an_admission_personal(
    dispatch_client: TestClient, world: World
) -> None:
    """The owner holds an active enrollment and is named in the requester and
    objective text; the admission names no mode and stays service."""

    number = next(_ISSUES)
    request_id = uuid.uuid4()

    admitted = _admit(
        dispatch_client,
        _body(
            world.agent,
            ADDRESS,
            number,
            request_id,
            requester=f"slack:{world.owner}:{OWNER_NAME}",
            objective=f"Run as {OWNER_NAME} ({world.owner}) on generation {OWNER_GENERATION}",
        ),
    )

    assert admitted.status_code == 200, admitted.text
    _assert_views(admitted.json(), tenant=world.tenant, mode="service", owner=None, generation=None)
    _assert_one_bound(
        number, request_id, tenant=world.tenant, mode="service", owner=None, generation=None
    )


AttemptBuilder = Callable[[World], tuple[UUID, str, str | None, dict[str, Any] | None]]
ControlBuilder = Callable[[World], tuple[UUID, str, dict[str, Any], UUID, UUID, int]]


def _home_control(w: World) -> tuple[UUID, str, dict[str, Any], UUID, UUID, int]:
    return w.agent, ADDRESS, w.owner_context(), w.tenant, w.owner, OWNER_GENERATION


def _other_control(w: World) -> tuple[UUID, str, dict[str, Any], UUID, UUID, int]:
    return (
        w.other_agent,
        OTHER_ADDRESS,
        w.foreign_context(),
        w.other_tenant,
        w.foreign,
        FOREIGN_GENERATION,
    )


REFUSALS: list[Any] = [
    pytest.param(
        "personal_owner_required",
        lambda w: (w.agent, ADDRESS, "personal", None),
        _home_control,
        id="personal-without-owner",
    ),
    pytest.param(
        "owner_context_unexpected",
        lambda w: (w.agent, ADDRESS, "service", w.owner_context()),
        _home_control,
        id="owner-with-service-mode",
    ),
    pytest.param(
        "owner_context_unexpected",
        lambda w: (w.agent, ADDRESS, None, w.owner_context()),
        _home_control,
        id="owner-without-mode",
    ),
    pytest.param(
        "owner_principal_inactive",
        lambda w: (w.agent, ADDRESS, "personal", _owner(w.tenant, w.disabled, 1)),
        _home_control,
        id="disabled-principal",
    ),
    pytest.param(
        "owner_principal_inactive",
        lambda w: (w.agent, ADDRESS, "personal", _owner(w.tenant, w.revoked, 1)),
        _home_control,
        id="revoked-principal",
    ),
    pytest.param(
        "owner_principal_missing",
        lambda w: (w.agent, ADDRESS, "personal", _owner(w.tenant, w.foreign, FOREIGN_GENERATION)),
        _home_control,
        id="principal-from-another-tenant",
    ),
    pytest.param(
        "owner_principal_missing",
        lambda w: (w.agent, ADDRESS, "personal", _owner(w.tenant, uuid.uuid4(), 1)),
        _home_control,
        id="absent-principal",
    ),
    pytest.param(
        "owner_tenant_mismatch",
        lambda w: (w.agent, ADDRESS, "personal", w.foreign_context()),
        _home_control,
        id="owner-tenant-differs-from-agent-tenant",
    ),
    pytest.param(
        "owner_tenant_mismatch",
        lambda w: (w.other_agent, OTHER_ADDRESS, "personal", w.owner_context()),
        _other_control,
        id="agent-in-another-tenant",
    ),
    pytest.param(
        "enrollment_inactive",
        lambda w: (w.agent, ADDRESS, "personal", _owner(w.tenant, w.unenrolled, 1)),
        _home_control,
        id="no-enrollment",
    ),
    pytest.param(
        "enrollment_inactive",
        lambda w: (w.agent, ADDRESS, "personal", _owner(w.tenant, w.lapsed, 1)),
        _home_control,
        id="inactive-enrollment",
    ),
    pytest.param(
        "enrollment_generation_mismatch",
        lambda w: (w.agent, ADDRESS, "personal", w.owner_context(OWNER_GENERATION - 1)),
        _home_control,
        id="generation-below-enrollment",
    ),
    pytest.param(
        "enrollment_generation_mismatch",
        lambda w: (w.agent, ADDRESS, "personal", w.owner_context(OWNER_GENERATION + 1)),
        _home_control,
        id="generation-above-enrollment",
    ),
]


@pytest.mark.parametrize("code, attempt, control", REFUSALS)
def test_unverified_owner_context_refuses_without_runnable_state(
    dispatch_client: TestClient,
    world: World,
    code: str,
    attempt: AttemptBuilder,
    control: ControlBuilder,
) -> None:
    number = next(_ISSUES)
    refused_id = uuid.uuid4()
    agent_id, address, mode, owner = attempt(world)

    refused = _admit(
        dispatch_client,
        _body(agent_id, address, number, refused_id, mode=mode, owner=owner),
    )

    _assert_refused(refused, code)
    _assert_no_runnable_state(number, refused_id)
    assert _count("SELECT count(*) AS n FROM curie.work_items") == 0
    assert _count("SELECT count(*) AS n FROM curie.execution_requests") == 0

    # Liveness: the valid owner context on the same agent setup and issue admits.
    control_agent, control_address, control_owner, tenant, principal, generation = control(world)
    admitted_id = uuid.uuid4()
    admitted = _admit(
        dispatch_client,
        _body(
            control_agent,
            control_address,
            number,
            admitted_id,
            mode="personal",
            owner=control_owner,
        ),
    )
    assert admitted.status_code == 200, admitted.text
    _assert_views(
        admitted.json(), tenant=tenant, mode="personal", owner=principal, generation=generation
    )
    _assert_one_bound(
        number, admitted_id, tenant=tenant, mode="personal", owner=principal, generation=generation
    )
    assert not _request_exists(refused_id)


def test_an_inactive_owner_tenant_refuses_until_it_is_active(
    dispatch_client: TestClient, world: World
) -> None:
    number = next(_ISSUES)
    refused_id = uuid.uuid4()
    _rows("UPDATE curie.tenants SET status = 'suspended' WHERE id = :id", {"id": world.tenant})

    refused = _admit(
        dispatch_client,
        _body(
            world.agent, ADDRESS, number, refused_id, mode="personal", owner=world.owner_context()
        ),
    )

    _assert_refused(refused, "owner_tenant_inactive")
    _assert_no_runnable_state(number, refused_id)

    _rows("UPDATE curie.tenants SET status = 'active' WHERE id = :id", {"id": world.tenant})
    admitted_id = uuid.uuid4()
    admitted = _admit(
        dispatch_client,
        _body(
            world.agent, ADDRESS, number, admitted_id, mode="personal", owner=world.owner_context()
        ),
    )
    assert admitted.status_code == 200, admitted.text
    _assert_one_bound(
        number,
        admitted_id,
        tenant=world.tenant,
        mode="personal",
        owner=world.owner,
        generation=OWNER_GENERATION,
    )


def test_an_unknown_model_auth_mode_is_rejected_before_admission(
    dispatch_client: TestClient, world: World
) -> None:
    number = next(_ISSUES)
    refused_id = uuid.uuid4()

    refused = _admit(
        dispatch_client,
        _body(
            world.agent,
            ADDRESS,
            number,
            refused_id,
            mode="subscription",
            owner=world.owner_context(),
        ),
    )

    assert refused.status_code == 422, refused.text
    _assert_no_runnable_state(number, refused_id)

    admitted_id = uuid.uuid4()
    admitted = _admit(
        dispatch_client,
        _body(
            world.agent, ADDRESS, number, admitted_id, mode="personal", owner=world.owner_context()
        ),
    )
    assert admitted.status_code == 200, admitted.text


def test_an_existing_personal_work_item_keeps_its_owner_and_generation(
    dispatch_client: TestClient, world: World
) -> None:
    number = next(_ISSUES)
    first = uuid.uuid4()
    original = _body(
        world.agent, ADDRESS, number, first, mode="personal", owner=world.owner_context()
    )
    admitted = _admit(dispatch_client, original)
    assert admitted.status_code == 200, admitted.text

    def unchanged() -> None:
        _assert_one_bound(
            number,
            first,
            tenant=world.tenant,
            mode="personal",
            owner=world.owner,
            generation=OWNER_GENERATION,
        )

    # The same owner context replays.
    replayed = _admit(dispatch_client, original)
    assert replayed.status_code == 200, replayed.text
    assert replayed.json()["replayed"] is True
    _assert_views(
        replayed.json(),
        tenant=world.tenant,
        mode="personal",
        owner=world.owner,
        generation=OWNER_GENERATION,
    )
    unchanged()

    # A different, fully valid and enrolled principal on the same issue.
    peer = _owner(world.tenant, world.peer, PEER_GENERATION)
    second = uuid.uuid4()
    _assert_refused(
        _admit(
            dispatch_client,
            _body(world.agent, ADDRESS, number, second, mode="personal", owner=peer),
        ),
        "owner_mismatch",
    )
    assert not _request_exists(second)
    unchanged()

    # The same request id replayed under that different principal.
    _assert_refused(
        _admit(
            dispatch_client,
            _body(world.agent, ADDRESS, number, first, mode="personal", owner=peer),
        ),
        "owner_mismatch",
    )
    unchanged()

    # A generation the owner's enrollment does not hold.
    third = uuid.uuid4()
    _assert_refused(
        _admit(
            dispatch_client,
            _body(
                world.agent,
                ADDRESS,
                number,
                third,
                mode="personal",
                owner=world.owner_context(OWNER_GENERATION + 1),
            ),
        ),
        "enrollment_generation_mismatch",
    )
    assert not _request_exists(third)
    unchanged()

    # The enrollment advances without a revalidation: naming the now current
    # generation still differs from the generation the WorkItem is bound to.
    _rows(
        "UPDATE curie.personal_claude_enrollments SET generation = :generation, "
        "version = version + 1, updated_at = now() "
        "WHERE tenant_id = :tenant AND principal_id = :principal",
        {"generation": OWNER_GENERATION + 1, "tenant": world.tenant, "principal": world.owner},
    )
    fourth = uuid.uuid4()
    _assert_refused(
        _admit(
            dispatch_client,
            _body(
                world.agent,
                ADDRESS,
                number,
                fourth,
                mode="personal",
                owner=world.owner_context(OWNER_GENERATION + 1),
            ),
        ),
        "enrollment_generation_mismatch",
    )
    assert not _request_exists(fourth)
    unchanged()


def test_an_existing_service_work_item_refuses_a_personal_owner(
    dispatch_client: TestClient, world: World
) -> None:
    number = next(_ISSUES)
    first = uuid.uuid4()
    admitted = _admit(dispatch_client, _body(world.agent, ADDRESS, number, first))
    assert admitted.status_code == 200, admitted.text

    second = uuid.uuid4()
    _assert_refused(
        _admit(
            dispatch_client,
            _body(
                world.agent, ADDRESS, number, second, mode="personal", owner=world.owner_context()
            ),
        ),
        "owner_mismatch",
    )
    assert not _request_exists(second)

    third = uuid.uuid4()
    _assert_refused(
        _admit(dispatch_client, _body(world.agent, ADDRESS, number, third, mode="personal")),
        "personal_owner_required",
    )
    assert not _request_exists(third)

    _assert_one_bound(
        number, first, tenant=world.tenant, mode="service", owner=None, generation=None
    )


# --- GitHub webhook ingress ------------------------------------------------------------


def _seed_default_tenant_owner() -> UUID:
    """An active principal of the default tenant (where ``POST /agents`` puts an
    agent) holding an active enrollment: a real owner the text can name."""

    async def body(connection: AsyncConnection) -> UUID:
        principal = await _principal(connection, DEFAULT_TENANT)
        await _enroll(connection, DEFAULT_TENANT, principal, generation=1)
        return principal

    return _run(body)


class _OwnerTextGitHub(GitHubAPI):
    """The GitHub fixture, also answering the permission read for a sender whose
    login is ``sender_login`` (an owner principal's UUID in these tests)."""

    def __init__(self) -> None:
        super().__init__()
        self.sender_login = SENDER

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == f"/repos/{REPO}/collaborators/{self.sender_login}/permission":
            self.permission_requests.append(request)
            return httpx.Response(
                200,
                json={
                    "permission": self.permission,
                    "user": {"id": self.permission_user_id, "login": self.sender_login},
                },
            )
        return super().handle(request)


GitHubFactory = Callable[[str | None], tuple[TestClient, _OwnerTextGitHub]]


@pytest.fixture
def github_factory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None
) -> Iterator[GitHubFactory]:
    """Start the factory app with ``FACTORY_MODEL_AUTH_MODE`` set (or unset for
    None), the GitHub fixture behind its HTTP client and one GitHub-bound agent."""

    with ExitStack() as stack:

        def start(mode: str | None) -> tuple[TestClient, _OwnerTextGitHub]:
            for key, value in factory_env(tmp_path).items():
                monkeypatch.setenv(key, value)
            if mode is None:
                monkeypatch.delenv("FACTORY_MODEL_AUTH_MODE", raising=False)
            else:
                monkeypatch.setenv("FACTORY_MODEL_AUTH_MODE", mode)
            get_settings.cache_clear()
            fake_installation_tokens(monkeypatch, _Credentials())
            api = _OwnerTextGitHub()
            client = stack.enter_context(TestClient(create_app()))
            external = httpx.AsyncClient(transport=httpx.MockTransport(api.handle))
            client.app.state.http_client = external
            stack.callback(client.portal.call, external.aclose)
            created = client.post(
                "/agents",
                headers={"X-API-Key": get_settings().api_key},
                json={
                    "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
                    "repo_full_name": REPO,
                    "channel": {"kind": "github", "address": REPO},
                },
            )
            assert created.status_code == 201, created.text
            return client, api

        yield start
    get_settings.cache_clear()


def _owner_text_event(number: int, owner: UUID) -> dict[str, Any]:
    """A signed-label event whose sender login, title and body name ``owner``."""

    payload = _issue_event(
        "labeled", number, label={"name": LABEL}, sender=_sender(login=str(owner))
    )
    payload["issue"]["title"] = f"Run as {OWNER_NAME} ({owner})"
    payload["issue"]["body"] = (
        f"owner_principal_id: {owner}\nowner: {OWNER_NAME}\n"
        f"tenant_id: {DEFAULT_TENANT}\nenrollment_generation: 1\nmodel_auth_mode: personal"
    )
    return payload


def _github_items(number: int) -> list[dict[str, Any]]:
    return _rows(
        f"SELECT id, {_TUPLE} FROM curie.work_items WHERE tracker_kind = 'github' "
        "AND tracker_scope_id = :scope AND tracker_issue_id = :number",
        {"scope": str(REPO_ID), "number": str(number)},
    )


def _github_requests(number: int) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.requester, r.tenant_id, r.model_auth_mode, r.owner_principal_id, "
        "r.enrollment_generation FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.tracker_kind = 'github' AND w.tracker_scope_id = :scope "
        "AND w.tracker_issue_id = :number ORDER BY r.sequence",
        {"scope": str(REPO_ID), "number": str(number)},
    )


def test_github_label_in_personal_mode_refuses_without_an_owner_even_when_text_names_one(
    github_factory: GitHubFactory, owned_rows: None
) -> None:
    owner = _seed_default_tenant_owner()
    client, api = github_factory("personal")
    api.sender_login = str(owner)
    number = next(_GITHUB_ISSUES)

    response = _post(client, "issues", _owner_text_event(number, owner))

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "factory_ignored"
    assert body["errors"] == [{"code": "personal_owner_required"}]
    assert _github_items(number) == []
    assert _github_requests(number) == []
    assert _count("SELECT count(*) AS n FROM curie.work_items") == 0
    assert _count("SELECT count(*) AS n FROM curie.execution_requests") == 0


@pytest.mark.parametrize("mode", [None, "service"], ids=["default", "explicit-service"])
def test_github_label_with_the_service_setting_admits_a_service_work_item(
    github_factory: GitHubFactory, owned_rows: None, mode: str | None
) -> None:
    owner = _seed_default_tenant_owner()
    client, api = github_factory(mode)
    api.sender_login = str(owner)
    number = next(_GITHUB_ISSUES)

    response = _post(client, "issues", _owner_text_event(number, owner))

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted"
    [item] = _github_items(number)
    assert _tuple(item) == (DEFAULT_TENANT, "service", None, None)
    [request] = _github_requests(number)
    assert _tuple(request) == (DEFAULT_TENANT, "service", None, None)
    # The owner's UUID reached the requester text and nothing else.
    assert str(owner) in request["requester"]


# --- port tracker intake (Jira) ----------------------------------------------------------


JiraIntake = Callable[[str | None], Intake]


@pytest.fixture
def jira_intake(request: pytest.FixtureRequest, clean_db: None) -> JiraIntake:
    """The tracker intake harness of ``test_factory_tracker_intake`` (Jira fake
    plus GitHub code host fake), with ``FACTORY_MODEL_AUTH_MODE`` set first."""

    build: Callable[[dict[str, Any]], Intake] = request.getfixturevalue("make_intake")
    monkeypatch: pytest.MonkeyPatch = request.getfixturevalue("monkeypatch")

    def start(mode: str | None) -> Intake:
        if mode is None:
            monkeypatch.delenv("FACTORY_MODEL_AUTH_MODE", raising=False)
        else:
            monkeypatch.setenv("FACTORY_MODEL_AUTH_MODE", mode)
        get_settings.cache_clear()
        return build(_document())

    return start


def _jira_items(issue: jira_fake.JiraIssue) -> list[dict[str, Any]]:
    return _rows(
        f"SELECT id, {_TUPLE} FROM curie.work_items "
        "WHERE tracker_kind = 'jira_cloud' AND tracker_issue_id = :id",
        {"id": str(issue.id)},
    )


def _jira_requests(issue: jira_fake.JiraIssue) -> list[dict[str, Any]]:
    return _rows(
        "SELECT r.id, r.requester, r.tenant_id, r.model_auth_mode, r.owner_principal_id, "
        "r.enrollment_generation FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.tracker_kind = 'jira_cloud' AND w.tracker_issue_id = :id ORDER BY r.sequence",
        {"id": str(issue.id)},
    )


def _owner_text_label(intake: Intake, owner: UUID) -> tuple[jira_fake.JiraIssue, UUID]:
    """The allowlisted starter, renamed to carry the owner's name and UUID,
    labels an issue whose summary and description name the owner. Returns the
    issue and the request id that label event admits under."""

    starter = intake.jira.add_user(STARTER_ID, f"{OWNER_NAME} {owner}")
    issue = intake.jira.seed_issue(
        f"Run as {OWNER_NAME} ({owner})",
        f"owner_principal_id: {owner}\nowner: {OWNER_NAME}\nmodel_auth_mode: personal",
    )
    event = intake.jira.add_label(issue.id, jira_fake.LABEL, starter)
    request_id = notice_request_id(
        MarkedNotice(
            issue=intake.ref(issue),
            marker=jira_fake.LABEL,
            actor=starter.actor,
            event_id=event,
            disposition=Disposition.ADMIT,
            cursor="",
            occurred_at=None,
        )
    )
    return issue, request_id


def test_tracker_intake_in_personal_mode_refuses_without_an_owner_even_when_text_names_one(
    jira_intake: JiraIntake, owned_rows: None, caplog: pytest.LogCaptureFixture
) -> None:
    owner = _seed_default_tenant_owner()
    intake = jira_intake("personal")
    intake.agent()
    issue, request_id = _owner_text_label(intake, owner)

    # The service logger does not propagate to root, where pytest's capture
    # handler lives, so the handler is attached to it by hand.
    service_logger = logging.getLogger("curie_api")
    service_logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.INFO, logger="curie_api"):
            intake.poll()
            intake.sweep()
    finally:
        service_logger.removeHandler(caplog.handler)

    assert _jira_items(issue) == []
    assert _jira_requests(issue) == []
    assert not _request_exists(request_id)
    assert _count("SELECT count(*) AS n FROM curie.work_items") == 0
    assert _count("SELECT count(*) AS n FROM curie.execution_requests") == 0
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "personal_owner_required" in logged


@pytest.mark.parametrize("mode", [None, "service"], ids=["default", "explicit-service"])
def test_tracker_intake_with_the_service_setting_admits_a_service_work_item(
    jira_intake: JiraIntake, owned_rows: None, mode: str | None
) -> None:
    owner = _seed_default_tenant_owner()
    intake = jira_intake(mode)
    agent = intake.agent()
    issue, request_id = _owner_text_label(intake, owner)

    intake.poll()

    [item] = _jira_items(issue)
    assert _tuple(item) == (DEFAULT_TENANT, "service", None, None)
    [request] = _jira_requests(issue)
    assert request["id"] == request_id
    assert _tuple(request) == (DEFAULT_TENANT, "service", None, None)
    assert str(owner) in request["requester"]
    assert (
        _count(
            "SELECT count(*) AS n FROM curie.work_items WHERE agent_id = :agent", {"agent": agent}
        )
        == 1
    )

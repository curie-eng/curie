"""The operator's connector requester identity flag (ADR 0270 decision 1).

@spec CONNECTOR-REQUESTER-1.

``agents.connector_requester_identity`` is an operator owned setting, default
off, versioned by ``connector_requester_identity_version``. It is written only
through ``PATCH /agents/{id}``, as a compare-and-set on that version, and only
alone: a flag write beside any other ``AgentUpdate`` field is refused before
anything commits, so the CAS is the request's single write and a losing request
changes nothing.

Enabling requires both the tenant's HTTP identity selection and at least one of
the agent's bindings listed in an operator receiver registration of the live
API runtime (a Slack ``ReceiverRegistrationV1`` or an
``EmailReceiverRegistrationV1`` whose ``binding_ids`` name it). HTTP
classification alone is not enough: a migrated or filtered role classifies a
binding as HTTP-required with no receiver behind it. Disabling needs neither.
The agent-owned access policy writer cannot touch the flag.

Every test drives the real router against the real Postgres, and every refusal
reads the stored row afterwards. The runtimes are the synthetic operator
registrations the HTTP invocation suites already build; no provider is called.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from _migration_support import sql_dicts, sql_rows
from curie_api import agent_role_policy
from curie_api.config import get_settings
from curie_api.identity.http_invocation import binding_requires_http_identity
from curie_api.main import create_app
from curie_api.models import AgentChannel
from fastapi.testclient import TestClient
from principal_adoption_support import ISSUER
from role_policy_support import change, run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests import test_http_email_receivers as email_receivers
from apps.api.tests import test_http_invocation_membership as membership

FLAG = "connector_requester_identity"
VERSION = "connector_requester_identity_expected_version"
OUT_VERSION = "connector_requester_identity_version"
CONFLICT = "agent.requester_identity_version_conflict"
NEEDS_HTTP = "requester_identity_requires_http_identity"
ALONE = "requester_identity_write_must_be_alone"
# Persisted as the selection's lookup URL only; nothing here calls a provider.
LOOKUP_URL = "https://lookup.example.test/v1/lookup"


# --- helpers ------------------------------------------------------------------


def _flag(value: Any, expected_version: Any) -> dict[str, Any]:
    """@spec CONNECTOR-REQUESTER-1: the one complete flag write."""
    return {FLAG: value, VERSION: expected_version}


def _headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


@contextmanager
def _app(runtime: Any | None) -> Iterator[TestClient]:
    """An API instance whose ``app.state.http_identity_runtime`` is ``runtime``."""
    with TestClient(create_app(http_identity_runtime=runtime)) as test_client:
        yield test_client


def _patch(client: TestClient, agent_id: str, body: dict[str, Any]) -> Any:
    return client.patch(f"/agents/{agent_id}", json=body, headers=_headers())


def _row(agent_id: str) -> dict[str, Any]:
    """The stored agent row, read independently of the router."""
    rows = sql_dicts(
        "SELECT connector_requester_identity AS flag, "
        "connector_requester_identity_version AS version, model, memory_writes, "
        "publication_policy, publication_policy_version "
        "FROM curie.agents WHERE id = :id",
        {"id": uuid.UUID(str(agent_id))},
    )
    assert len(rows) == 1, rows
    return rows[0]


def _stored(agent_id: str) -> tuple[bool, int]:
    row = _row(agent_id)
    return row["flag"], row["version"]


def _seed_stored(agent_id: str, flag: bool, version: int) -> None:
    """Fixture state: a flag an earlier operator write would have left."""
    sql_rows(
        "UPDATE curie.agents SET connector_requester_identity = :flag, "
        "connector_requester_identity_version = :version WHERE id = :id",
        {"flag": flag, "version": version, "id": uuid.UUID(str(agent_id))},
    )


def _selected(agent_id: str) -> bool:
    """Whether the agent's tenant holds an HTTP identity selection."""
    rows = sql_dicts(
        "SELECT p.http_selection IS NOT NULL AS selected "
        "FROM curie.agents a JOIN curie.tenant_identity_providers p "
        "ON p.tenant_id = a.tenant_id WHERE a.id = :id",
        {"id": uuid.UUID(str(agent_id))},
    )
    return bool(rows and rows[0]["selected"])


def _clear_selection(tenant_id: str) -> None:
    """Leave the tenant on its local authority: no HTTP identity selection."""
    sql_rows(
        "UPDATE curie.tenant_identity_providers SET http_selection = NULL, "
        "governed_domains = CAST(:domains AS jsonb), managed_by = 'synthetic-directory' "
        "WHERE tenant_id = :tenant",
        {"domains": json.dumps(["example.test"]), "tenant": uuid.UUID(tenant_id)},
    )


def _classified(binding_id: str, runtime: Any | None) -> bool:
    """The existing per-binding HTTP classification, as a premise check only."""
    result: list[bool] = []

    async def check(factory: async_sessionmaker[AsyncSession]) -> None:
        async with factory() as session:
            binding = await session.get(AgentChannel, uuid.UUID(binding_id))
            assert binding is not None
            result.append(await binding_requires_http_identity(session, runtime, binding))

    run(check)
    return result[0]


def _plain_agent(client: TestClient, name: str) -> str:
    """An agent created through the API: default tenant, no HTTP selection."""
    created = client.post(
        "/agents",
        json={"name": name, "channel": {"kind": "slack", "address": "C0EXAMPLE1"}},
        headers=_headers(),
    )
    assert created.status_code == 201, created.text
    agent_id: str = created.json()["id"]
    assert not _selected(agent_id)
    return agent_id


def _registered_slack() -> tuple[dict[str, Any], Any]:
    """An adopted HTTP tenant whose Slack binding an operator registration lists."""
    scope = membership._scope(LOOKUP_URL)
    runtime = membership._runtime(membership._runtime_api(), scope)
    assert _selected(scope["agent"])
    return scope, runtime


def _both(first: Callable[[], Any], second: Callable[[], Any]) -> tuple[Any, Any]:
    """Run two request thunks in parallel threads and return both responses."""

    async def race() -> list[Any]:
        return list(await asyncio.gather(asyncio.to_thread(first), asyncio.to_thread(second)))

    left, right = asyncio.run(race())
    return left, right


def _detail_text(response: Any) -> str:
    return json.dumps(response.json().get("detail"))


# --- read surface --------------------------------------------------------------


def test_a_new_agent_reads_the_flag_off_at_version_one(client: TestClient, clean_db: None) -> None:
    """@spec CONNECTOR-REQUESTER-1: the default is off, and AgentOut carries both fields."""
    created = client.post(
        "/agents",
        json={"name": "requester-default", "channel": {"kind": "slack", "address": "C0EXAMPLE1"}},
        headers=_headers(),
    )
    assert created.status_code == 201, created.text
    assert created.json()[FLAG] is False
    assert created.json()[OUT_VERSION] == 1
    agent_id = created.json()["id"]

    read = client.get(f"/agents/{agent_id}", headers=_headers())
    assert read.status_code == 200, read.text
    assert read.json()[FLAG] is False
    assert read.json()[OUT_VERSION] == 1
    assert _stored(agent_id) == (False, 1)


# --- write validation ---------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({FLAG: True}, id="enable-without-version"),
        pytest.param({FLAG: False}, id="disable-without-version"),
        pytest.param({FLAG: True, VERSION: None}, id="explicit-null-version"),
        pytest.param({VERSION: 1}, id="version-without-flag"),
        pytest.param({FLAG: None, VERSION: 1}, id="explicit-null-flag"),
        pytest.param({FLAG: True, VERSION: 0}, id="version-below-one"),
    ],
)
def test_an_incomplete_flag_write_is_422_and_changes_nothing(
    clean_db: None, body: dict[str, Any]
) -> None:
    """@spec CONNECTOR-REQUESTER-1: the flag and its expected version travel together.

    The agent is eligible to enable, so the incomplete body is the only reason
    to refuse.
    """
    scope, runtime = _registered_slack()
    before = _row(scope["agent"])
    with _app(runtime) as api:
        response = _patch(api, scope["agent"], body)
    assert response.status_code == 422, response.text
    assert _row(scope["agent"]) == before
    assert _stored(scope["agent"]) == (False, 1)


@pytest.mark.parametrize(
    ("other", "value"),
    [
        pytest.param({"model": "claude-sonnet-5"}, True, id="model-beside-enable"),
        pytest.param({"memory_writes": True}, True, id="memory-writes-beside-enable"),
        pytest.param({"publication_policy": "auto"}, False, id="publication-beside-disable"),
    ],
)
def test_a_flag_write_beside_another_field_is_422_and_commits_nothing(
    clean_db: None, other: dict[str, Any], value: bool
) -> None:
    """@spec CONNECTOR-REQUESTER-1: a flag write is the only mutation in its request.

    Every other field block in ``update_agent`` commits on its own, so a flag
    write sharing a request could leave the other field persisted from a
    request answered with a refusal. The co-sent field must not commit.
    """
    scope, runtime = _registered_slack()
    before = _row(scope["agent"])
    with _app(runtime) as api:
        response = _patch(api, scope["agent"], {**other, **_flag(value, 1)})
    assert response.status_code == 422, response.text
    assert ALONE in _detail_text(response)
    assert _row(scope["agent"]) == before
    assert _stored(scope["agent"]) == (False, 1)


def test_a_write_without_the_flag_still_commits_and_keeps_the_flag(
    client: TestClient, clean_db: None
) -> None:
    """@spec CONNECTOR-REQUESTER-1: liveness. Other fields still write alone."""
    agent_id = _plain_agent(client, "requester-liveness")
    _seed_stored(agent_id, True, 4)
    response = _patch(client, agent_id, {"model": "claude-sonnet-5"})
    assert response.status_code == 200, response.text
    assert response.json()["model"] == "claude-sonnet-5"
    assert response.json()[FLAG] is True
    assert response.json()[OUT_VERSION] == 4
    row = _row(agent_id)
    assert (row["flag"], row["version"], row["model"]) == (True, 4, "claude-sonnet-5")


# --- enabling requires HTTP identity and a registered receiver ---------------


@pytest.mark.parametrize("case", ["no_identity_provider", "registered_but_selection_cleared"])
def test_enabling_without_an_http_selection_is_422_and_changes_nothing(
    client: TestClient, clean_db: None, case: str
) -> None:
    """@spec CONNECTOR-REQUESTER-1: HTTP identity mode is a precondition.

    The second case keeps a receiver registration listing the binding, so the
    missing tenant selection is the only reason to refuse.
    """
    if case == "no_identity_provider":
        agent_id, runtime = _plain_agent(client, "requester-no-selection"), None
    else:
        scope, runtime = _registered_slack()
        agent_id = scope["agent"]
        _clear_selection(scope["tenant"])
        assert not _selected(agent_id)
    with _app(runtime) as api:
        response = _patch(api, agent_id, _flag(True, 1))
    assert response.status_code == 422, response.text
    assert NEEDS_HTTP in _detail_text(response)
    assert _stored(agent_id) == (False, 1)


@pytest.mark.parametrize(
    "case", ["role_only_without_runtime", "identity_registered_binding_unlisted"]
)
def test_an_http_classified_binding_no_registration_lists_is_422(clean_db: None, case: str) -> None:
    """@spec CONNECTOR-REQUESTER-1: classification is not registration.

    The agent's migrated, filtered invocation role classifies its binding as
    HTTP-required. In the second case a Slack registration names the binding's
    channel identity (which classifies too) but lists another binding id, so
    no receiver registration lists this one.
    """
    scope = membership._scope(LOOKUP_URL)
    runtime = None
    if case == "identity_registered_binding_unlisted":
        unlisted = {**scope, "binding": str(uuid.uuid4())}
        runtime = membership._runtime(membership._runtime_api(), unlisted)
    assert _selected(scope["agent"])
    assert _classified(scope["binding"], runtime) is True
    with _app(runtime) as api:
        response = _patch(api, scope["agent"], _flag(True, 1))
    assert response.status_code == 422, response.text
    assert NEEDS_HTTP in _detail_text(response)
    assert _stored(scope["agent"]) == (False, 1)


def test_enabling_with_a_registered_slack_binding_bumps_the_version(clean_db: None) -> None:
    """@spec CONNECTOR-REQUESTER-1: selection plus a listed Slack binding enables."""
    scope, runtime = _registered_slack()
    with _app(runtime) as api:
        response = _patch(api, scope["agent"], _flag(True, 1))
        assert response.status_code == 200, response.text
        assert response.json()[FLAG] is True
        assert response.json()[OUT_VERSION] == 2
        read = api.get(f"/agents/{scope['agent']}", headers=_headers())
    assert read.status_code == 200, read.text
    assert (read.json()[FLAG], read.json()[OUT_VERSION]) == (True, 2)
    assert _stored(scope["agent"]) == (True, 2)


def test_enabling_with_an_email_receiver_registration_succeeds(clean_db: None) -> None:
    """@spec CONNECTOR-REQUESTER-1: an email registration listing the binding counts.

    The runtime registers email receivers only; the agent's Slack binding is
    listed by nothing, its email binding by the email registration.
    """
    scope = email_receivers._email_scope(LOOKUP_URL)
    runtime = email_receivers._email_runtime(email_receivers._email_api(), scope)
    assert _selected(scope["agent"])
    with _app(runtime) as api:
        response = _patch(api, scope["agent"], _flag(True, 1))
    assert response.status_code == 200, response.text
    assert (response.json()[FLAG], response.json()[OUT_VERSION]) == (True, 2)
    assert _stored(scope["agent"]) == (True, 2)


# --- compare-and-set ----------------------------------------------------------


def test_a_stale_expected_version_is_409_and_changes_nothing(clean_db: None) -> None:
    """@spec CONNECTOR-REQUESTER-1: the AGENTS.md versioned-row conflict test."""
    scope, runtime = _registered_slack()
    agent_id = scope["agent"]
    with _app(runtime) as api:
        enabled = _patch(api, agent_id, _flag(True, 1))
        assert enabled.status_code == 200, enabled.text
        assert _stored(agent_id) == (True, 2)
        for body in (_flag(False, 1), _flag(True, 1), _flag(False, 3)):
            response = _patch(api, agent_id, body)
            assert response.status_code == 409, (body, response.text)
            assert response.json()["detail"]["code"] == CONFLICT
            assert _stored(agent_id) == (True, 2)


def test_disabling_never_needs_http_identity(client: TestClient, clean_db: None) -> None:
    """@spec CONNECTOR-REQUESTER-1: off is always writable; every write moves the version."""
    agent_id = _plain_agent(client, "requester-disable")
    response = _patch(client, agent_id, _flag(False, 1))
    assert response.status_code == 200, response.text
    assert (response.json()[FLAG], response.json()[OUT_VERSION]) == (False, 2)
    assert _stored(agent_id) == (False, 2)

    stale = _patch(client, agent_id, _flag(False, 1))
    assert stale.status_code == 409, stale.text
    assert stale.json()["detail"]["code"] == CONFLICT
    assert _stored(agent_id) == (False, 2)


def test_disabling_after_enabling_needs_only_the_current_version(clean_db: None) -> None:
    """@spec CONNECTOR-REQUESTER-1: the registration and selection can be gone by then."""
    scope, runtime = _registered_slack()
    agent_id = scope["agent"]
    with _app(runtime) as api:
        enabled = _patch(api, agent_id, _flag(True, 1))
    assert enabled.status_code == 200, enabled.text
    _clear_selection(scope["tenant"])
    with _app(None) as api:
        disabled = _patch(api, agent_id, _flag(False, 2))
    assert disabled.status_code == 200, disabled.text
    assert (disabled.json()[FLAG], disabled.json()[OUT_VERSION]) == (False, 3)
    assert _stored(agent_id) == (False, 3)


@pytest.mark.parametrize(
    "values",
    [
        pytest.param((True, True), id="both-enable"),
        pytest.param((True, False), id="enable-disable"),
    ],
)
def test_concurrent_writers_with_one_expected_version_commit_exactly_once(
    clean_db: None, values: tuple[bool, bool]
) -> None:
    """@spec CONNECTOR-REQUESTER-1: two operators, two API instances, one version.

    Two app instances so one client's portal cannot serialize the overlap.
    Whichever lands first commits and moves the version to 2; the other names a
    version that is no longer stored and changes nothing.
    """
    scope, _ = _registered_slack()
    agent_id = scope["agent"]
    module = membership._runtime_api()
    with (
        _app(membership._runtime(module, scope)) as first,
        _app(membership._runtime(module, scope)) as second,
    ):
        left, right = _both(
            lambda: _patch(first, agent_id, _flag(values[0], 1)),
            lambda: _patch(second, agent_id, _flag(values[1], 1)),
        )
    statuses = sorted([left.status_code, right.status_code])
    assert statuses == [200, 409], (left.text, right.text)
    winner, loser = (left, right) if left.status_code == 200 else (right, left)
    assert loser.json()["detail"]["code"] == CONFLICT
    assert winner.json()[OUT_VERSION] == 2
    assert _stored(agent_id) == (winner.json()[FLAG], 2)


# --- the access policy writer cannot change the flag --------------------------


@pytest.mark.parametrize("key", [FLAG, VERSION, OUT_VERSION])
def test_the_access_policy_writer_cannot_change_the_flag(
    client: TestClient, clean_db: None, key: str
) -> None:
    """@spec CONNECTOR-REQUESTER-1: agent-owned access writes cannot toggle it."""
    agent_id = _plain_agent(client, "requester-access-policy")
    _seed_stored(agent_id, True, 5)
    path = f"/agents/{agent_id}/access-policy"
    policy = {"invocation_team_id": None, "approver_team_id": None, "expected_version": 0}

    refused = client.put(
        path, json={**policy, key: False if key == FLAG else 1}, headers=_headers()
    )
    assert refused.status_code == 422, refused.text
    assert _stored(agent_id) == (True, 5)
    assert client.get(path, headers=_headers()).json()["version"] == 0

    written = client.put(path, json=policy, headers=_headers())
    assert written.status_code == 200, written.text
    assert written.json()["version"] == 1
    assert _stored(agent_id) == (True, 5)


def test_a_role_filter_write_leaves_the_flag(clean_db: None) -> None:
    """@spec CONNECTOR-REQUESTER-1: the agent-owned filter writer cannot toggle it."""
    scope, runtime = _registered_slack()
    agent_id = scope["agent"]
    with _app(runtime) as api:
        enabled = _patch(api, agent_id, _flag(True, 1))
    assert enabled.status_code == 200, enabled.text
    versions: list[int] = []

    async def write(factory: async_sessionmaker[AsyncSession]) -> None:
        async with factory() as session, session.begin():
            result = await agent_role_policy.apply_role_change(
                session,
                change=change(
                    uuid.UUID(scope["tenant"]),
                    uuid.UUID(agent_id),
                    version=scope["role_version"],
                    operation="set_filter",
                    filter={
                        "entries": [
                            {
                                "kind": "issuer_group",
                                "group": {"issuer": ISSUER, "id": "SyntheticOtherGroup"},
                            }
                        ],
                    },
                ),
            )
            versions.append(result.version)

    run(write)
    assert versions == [scope["role_version"] + 1]
    assert _stored(agent_id) == (True, 2)

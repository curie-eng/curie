"""ADR 0272 decision 1: an operator route may choose one selected approver's DM.

@spec APPROVER-DM-1.

Every write goes through the real ``PATCH /agents/{id}`` full ``approval_routes``
replacement, versioned on the agent's requester-identity configuration, exactly
like the ADR 0271 registration suite. The API's live HTTP identity runtime
registers one Slack receiver for the binding's bot identity. Each refusal is a
422 whose detail carries the pinned code, reads the stored row afterwards
(unchanged map, unchanged version) and is followed by the neighboring valid
write, which must be accepted. Synthetic, reserved example values only.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from approver_dm_support import DM_MODE, add_identity, dm_resolution, dm_routes, write_dm
from approver_ticket_support import (
    OTHER_ROUTE,
    ROUTE,
    agent_state,
    operator_app,
    prepare,
    readback,
    refused_with,
)
from approver_ticket_support import operator_case as operator_case
from curie_api.models import AgentChannel, ChannelIdentity
from role_policy_support import run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

NOTIFY = {"kind": "slack", "address": "C0EXAMPLENOTE"}


def _stored(agent: str) -> tuple[Any, int]:
    """@spec APPROVER-DM-1: the stored route map and configuration version."""
    state = agent_state(agent)
    return state["approval_routes"], int(state["version"])


def _refused(
    case: SimpleNamespace, routes: dict[str, Any], code: str | None,
) -> None:
    """A 422 refusal (with ``code`` in its detail when given) that stores nothing.

    @spec APPROVER-DM-1.
    """
    before = _stored(case.scope["agent"])
    response = write_dm(case.client, case.scope, routes)
    if code is None:
        assert response.status_code == 422, response.text
    else:
        refused_with(response, 422, code)
    assert _stored(case.scope["agent"]) == before


def _accepted_readback(case: SimpleNamespace, identity: str, route: str = ROUTE) -> None:
    """The DM resolution reads back and is stored exactly as the pinned object.

    @spec APPROVER-DM-1.
    """
    agent = case.scope["agent"]
    for view in (readback(case.client, agent)["approval_routes"], agent_state(agent)[
        "approval_routes"
    ]):
        assert view[route]["resolution"] == {"mode": DM_MODE, "identity": identity}, view
        assert "notification" not in view[route] or view[route]["notification"] is None, view


@pytest.fixture
def dm_operator(operator_case: SimpleNamespace) -> SimpleNamespace:
    """@spec APPROVER-DM-1: the 0271 operator agent, prepared for a selector registration."""
    prepare(operator_case.client, operator_case.scope)
    return operator_case


# --- liveness -------------------------------------------------------------------------


def test_a_dynamic_route_with_a_registered_identity_accepts_the_dm_mode(
    dm_operator: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-1: selector registered, identity configured and received."""
    case = dm_operator
    identity = case.scope["adapter"]
    before = case.scope["config_version"]
    response = write_dm(case.client, case.scope, dm_routes(case.scope, case.key))
    assert response.status_code == 200, response.text
    assert case.scope["config_version"] == before + 1
    _accepted_readback(case, identity)
    assert "approver_ticket" in agent_state(case.scope["agent"])["approval_routes"][ROUTE]


def test_the_default_identity_reads_back_exactly(dm_operator: SimpleNamespace) -> None:
    """@spec APPROVER-DM-1: ``identity: default`` is stored and read back, never dropped.

    The binding's bot identity is renamed ``default`` (as the email suite does)
    and the API is restarted with a receiver registered under that name.
    """
    case = dm_operator
    scope = case.scope

    async def rename(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec APPROVER-DM-1."""
        async with factory() as session, session.begin():
            identity = await session.get(ChannelIdentity, uuid.UUID(scope["identity"]))
            binding = await session.get(AgentChannel, uuid.UUID(scope["binding"]))
            assert identity is not None and binding is not None
            identity.name = "default"
            binding.adapter = "default"

    run(rename)
    scope["adapter"] = "default"
    with operator_app(case.runtime_for()) as client:
        response = write_dm(client, scope, dm_routes(scope, case.key, identity="default"))
        assert response.status_code == 200, response.text
        view = readback(client, scope["agent"])["approval_routes"][ROUTE]
        assert view["resolution"] == {"mode": "approver_dm", "identity": "default"}, view
    stored = agent_state(scope["agent"])["approval_routes"][ROUTE]
    assert stored["resolution"] == {"mode": "approver_dm", "identity": "default"}, stored


# --- refusals ------------------------------------------------------------------------


def test_a_dm_resolution_without_a_selector_registration_is_refused(
    dm_operator: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-1: a static route never chooses a DM (``approver_dm_static_route``).

    Both a selector-free owning route and a DM on the looser route while the
    owning route carries the registration refuse; the DM on the registered
    route is then accepted.
    """
    case = dm_operator
    scope = case.scope
    _refused(case, dm_routes(scope, case.key, ticket=False), "approver_dm_static_route")
    _refused(case, dm_routes(scope, case.key, route=OTHER_ROUTE), "approver_dm_static_route")
    assert write_dm(case.client, scope, dm_routes(scope, case.key)).status_code == 200
    _accepted_readback(case, scope["adapter"])


def test_a_dm_resolution_with_a_notification_is_refused(dm_operator: SimpleNamespace) -> None:
    """@spec APPROVER-DM-1: the DM carries no notification (``approver_dm_notification``)."""
    case = dm_operator
    scope = case.scope
    _refused(case, dm_routes(scope, case.key, notification=NOTIFY), "approver_dm_notification")
    assert write_dm(case.client, scope, dm_routes(scope, case.key)).status_code == 200
    _accepted_readback(case, scope["adapter"])


def test_an_identity_without_a_registered_receiver_is_refused(
    dm_operator: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-1: the identity must be configured and received.

    A name no identity row carries, and an active identity row in the tenant
    for which the API registers no receiver, both refuse with
    ``approver_dm_receiver_missing``.
    """
    case = dm_operator
    scope = case.scope
    unregistered = add_identity(scope, "synthetic-dm-unregistered-" + uuid.uuid4().hex[:12])
    for name in ("synthetic-dm-missing", unregistered["adapter"]):
        _refused(case, dm_routes(scope, case.key, identity=name), "approver_dm_receiver_missing")
    assert write_dm(case.client, scope, dm_routes(scope, case.key)).status_code == 200
    _accepted_readback(case, scope["adapter"])


@pytest.mark.parametrize(
    "resolution",
    [
        pytest.param({"mode": "approver_dm"}, id="no-identity"),
        pytest.param({"mode": "approver_dm", "identity": ""}, id="empty-identity"),
        pytest.param({"mode": "approver_dm", "identity": "Not A Slug"}, id="bad-slug"),
        pytest.param({"mode": "approver-dm", "identity": "default"}, id="unknown-mode"),
        pytest.param({"identity": "default"}, id="no-mode"),
        pytest.param(
            {"mode": "approver_dm", "identity": "default", "address": "C0EXAMPLEDM"},
            id="extra-address",
        ),
        pytest.param(
            {"mode": "approver_dm", "identity": "default", "kind": "slack"}, id="extra-kind",
        ),
        pytest.param(
            {"mode": "approver_dm", "identity": "default", "recipient": "U0EXAMPLEA"},
            id="extra-recipient",
        ),
    ],
)
def test_malformed_dm_resolutions_are_refused(
    dm_operator: SimpleNamespace, resolution: dict[str, Any],
) -> None:
    """@spec APPROVER-DM-1: the strict mode object; no mixed or partial form is guessed."""
    case = dm_operator
    scope = case.scope
    if "identity" in resolution and resolution["identity"] == "default":
        resolution = {**resolution, "identity": scope["adapter"]}
    _refused(case, dm_routes(scope, case.key, resolution=resolution), None)
    assert write_dm(
        case.client, scope, dm_routes(scope, case.key, resolution=dm_resolution(scope["adapter"])),
    ).status_code == 200
    _accepted_readback(case, scope["adapter"])

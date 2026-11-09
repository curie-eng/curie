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
    ROUTES_EXPECTED,
    agent_state,
    operator_app,
    patch_agent,
    prepare,
    readback,
    refused_with,
    route_map,
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


# --- review r1 F1: a refused DM route commits no sibling field --------------------------

# The fields ``PATCH /agents/{id}`` writes through independently committing
# helpers BEFORE its ``approval_routes`` block (routers/agents.py): a refusal
# judged only at that block would leave every one of them persisted from a
# request the API answered 422.
SIBLINGS_BEFORE: dict[str, Any] = {
    "model": "claude-sonnet-5",
    "reviewer_model": "anthropic/claude-opus-5.5",
    "thinking": "enabled:2000",
    "execution_deadline_seconds": 3600,
    "memory": True,
    "memory_writes": True,
    "approval_required_tools": ["mcp__access__execute", "Bash"],
}
SIBLINGS_EDIT: dict[str, Any] = {
    "model": "claude-opus-5",
    "reviewer_model": "anthropic/claude-sonnet-5",
    "thinking": "adaptive",
    "execution_deadline_seconds": 900,
    "memory": False,
    "memory_writes": False,
    "approval_required_tools": [],
}


def _with_siblings(case: SimpleNamespace) -> dict[str, Any]:
    """@spec APPROVER-DM-1: gates and the other early fields set; returns the GET view."""
    response = patch_agent(case.client, case.scope["agent"], SIBLINGS_BEFORE)
    assert response.status_code == 200, response.text
    view = readback(case.client, case.scope["agent"])
    for field, value in SIBLINGS_BEFORE.items():
        assert view[field] == value, (field, view[field])
    assert view["approval_routes"] and "approver_ticket" not in view["approval_routes"][ROUTE]
    return view


def _combined(case: SimpleNamespace, routes: dict[str, Any], *, versioned: bool = False) -> Any:
    """@spec APPROVER-DM-1: one PATCH carrying the sibling edit and the route map."""
    body = {**SIBLINGS_EDIT, "approval_routes": routes}
    if versioned:
        body[ROUTES_EXPECTED] = case.scope["config_version"]
    return patch_agent(case.client, case.scope["agent"], body)


def _unchanged(case: SimpleNamespace, before: dict[str, Any], stored: Any) -> None:
    """@spec APPROVER-DM-1: a fresh GET and the stored row show the agent exactly as before."""
    after = readback(case.client, case.scope["agent"])
    for field in (*SIBLINGS_BEFORE, "approval_routes", "connector_requester_identity_version"):
        assert after[field] == before[field], (
            f"{field} was committed by a request the API refused: "
            f"{before[field]!r} -> {after[field]!r}"
        )
    assert after == before
    assert _stored(case.scope["agent"]) == stored


DM_CODES = {"approver_dm_static_route", "approver_dm_notification", "approver_dm_receiver_missing"}


@pytest.mark.parametrize(
    "variant",
    [
        pytest.param("static", id="static-route"),
        pytest.param("notification", id="static-with-notification"),
        pytest.param("receiver", id="static-with-missing-receiver"),
    ],
)
def test_a_refused_dm_route_persists_no_sibling_field(
    dm_operator: SimpleNamespace, variant: str,
) -> None:
    """@spec APPROVER-DM-1 (review r1 F1): the DM refusal is judged before any field commits.

    An ordinary agent (no selector registration) with gates and the other
    early-committed fields set receives one unversioned PATCH that clears its
    gates (``approval_required_tools: []``), changes every other early field
    and writes a selector-free DM route: plain, with a notification, or naming
    an identity with no receiver. Only a selector-free map can share a PATCH
    with sibling fields (a versioned route write must be sent alone), so each
    is refused 422 with an approver-DM code (``approver_dm_static_route`` for
    the plain form; the other two may be judged by either of their codes).
    A fresh GET shows the agent exactly as before: gates kept, no sibling
    changed, route map and configuration version unchanged. The same sibling
    edit with the neighboring valid selector-free map then succeeds, so the
    refusal is caused by the route alone.
    """
    case = dm_operator
    scope = case.scope
    before = _with_siblings(case)
    stored = _stored(scope["agent"])
    routes = {
        "static": lambda: dm_routes(scope, case.key, ticket=False),
        "notification": lambda: dm_routes(scope, case.key, ticket=False, notification=NOTIFY),
        "receiver": lambda: dm_routes(
            scope, case.key, ticket=False, identity="synthetic-dm-missing",
        ),
    }[variant]()

    response = _combined(case, routes)
    assert response.status_code == 422, response.text
    found = {code for code in DM_CODES if code in response.text}
    assert found, response.text
    if variant == "static":
        assert found == {"approver_dm_static_route"}, response.text
    _unchanged(case, before, stored)

    accepted = _combined(case, route_map(scope["address"]))
    assert accepted.status_code == 200, accepted.text
    view = readback(case.client, scope["agent"])
    for field, value in SIBLINGS_EDIT.items():
        assert view[field] == (value or None if field == "approval_required_tools" else value), (
            field, view[field],
        )


@pytest.mark.parametrize(
    ("variant", "code"),
    [
        pytest.param("notification", "approver_dm_notification", id="notification"),
        pytest.param("receiver", "approver_dm_receiver_missing", id="receiver-missing"),
    ],
)
def test_a_versioned_dm_refusal_with_siblings_commits_nothing(
    dm_operator: SimpleNamespace, variant: str, code: str,
) -> None:
    """@spec APPROVER-DM-1 (review r1 F1): the selector-carrying forms commit nothing either.

    A DM route with its ``approver_ticket`` is a versioned write, which the API
    refuses to combine with other fields; sent with the sibling edit it is
    refused 422 before any field commits. Sent alone it answers its pinned code,
    and the agent is still exactly as before.
    """
    case = dm_operator
    scope = case.scope
    before = _with_siblings(case)
    stored = _stored(scope["agent"])
    routes = {
        "notification": lambda: dm_routes(scope, case.key, notification=NOTIFY),
        "receiver": lambda: dm_routes(scope, case.key, identity="synthetic-dm-missing"),
    }[variant]()
    combined = _combined(case, routes, versioned=True)
    assert combined.status_code == 422, combined.text
    _unchanged(case, before, stored)
    refused_with(write_dm(case.client, scope, routes), 422, code)
    _unchanged(case, before, stored)

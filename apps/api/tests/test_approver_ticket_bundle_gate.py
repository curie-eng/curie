"""ADR 0271 decision 2, gate half: the serving bundle must gate the registered pair.

@spec REQUEST-APPROVAL-2.

A registration names a connector/tool pair and the route that owns it. It is
accepted only when every active deployment version's effective
(last-declaration-wins) approval route map sends ``mcp__<connector>__<tool>``
to exactly that route, and a serving-bundle change that would move or drop the
gate is refused at every entry point (plan D5, D6). The operator routes are
driven over HTTP; the git push entry point shares
``deploy.check_routes_from_bytes`` and is driven through it directly, as
``test_approval_route_config_check.py`` does for the unbound-route half. The
frozen normalization vector is executed against the new map reader.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Coroutine
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from _migration_support import sql_dicts
from approver_ticket_support import (
    LIVE,
    OTHER_ROUTE,
    ROUTE,
    SEED_ROUTE,
    active_deployments,
    agent_state,
    api_headers,
    bundle,
    deploy,
    prepare,
    refused_with,
    register,
    registered,
    route_map,
    serve,
    stored_version,
    write_routes,
)
from approver_ticket_support import operator_case as operator_case
from curie_api import bundles
from curie_api import deploy as deploy_checks

MISMATCH = "approver_ticket.bundle_gate_mismatch"
VECTOR = Path(__file__).resolve().parents[3] / "tests/vectors/approval-route-normalization.json"
KEEPS_GATE = ((LIVE, ROUTE), ("Bash", OTHER_ROUTE))


def refusal_code(check: Coroutine[Any, Any, None]) -> str | None:
    """The code a byte-holding route check refuses with, or None when it accepts."""
    try:
        asyncio.run(check)
    except Exception as refusal:  # noqa: BLE001 - the plan names the code, not the class.
        return str(getattr(refusal, "code", type(refusal).__name__))
    return None


def test_registration_refuses_a_bundle_gating_the_pair_on_another_route(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-2: the runner's effective route decides, not the registration."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope, gates=((LIVE, OTHER_ROUTE), ("Bash", ROUTE)))
    before = agent_state(agent)
    refused_with(register(client, scope, case.key), 422, MISMATCH)
    assert agent_state(agent) == before
    accepted = register(client, scope, case.key, on=OTHER_ROUTE)
    assert accepted.status_code == 200, accepted.text


def test_every_active_version_must_gate_the_pair(operator_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-2: an ungated serving version refuses until it stops serving."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope, gates=(("Bash", OTHER_ROUTE),))
    before = agent_state(agent)
    refused_with(register(client, scope, case.key), 422, MISMATCH)
    serve(client, agent, KEEPS_GATE)
    assert len(active_deployments(agent)) == 2
    refused_with(register(client, scope, case.key), 422, MISMATCH)
    assert agent_state(agent) == before
    ended = client.delete(f"/deployments/{scope['deployment_id']}", headers=api_headers())
    assert ended.status_code == 204, ended.text
    accepted = register(client, scope, case.key)
    assert accepted.status_code == 200, accepted.text


def test_registration_needs_a_serving_bundle(operator_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-2: no active deployment means nothing proves the gate."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope, serve_bundle=False)
    assert active_deployments(agent) == []
    before = agent_state(agent)
    refused_with(register(client, scope, case.key), 422, "approver_ticket.no_serving_bundle")
    assert agent_state(agent) == before
    serve(client, agent, KEEPS_GATE)
    accepted = register(client, scope, case.key)
    assert accepted.status_code == 200, accepted.text


def test_duplicate_gates_use_the_last_declaration(operator_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-2: the earlier route of a repeated gate can never be raised."""
    case = operator_case
    client, scope = case.client, case.scope
    prepare(client, scope, gates=((LIVE, OTHER_ROUTE), (LIVE, ROUTE)))
    refused_with(register(client, scope, case.key, on=OTHER_ROUTE), 422, MISMATCH)
    accepted = register(client, scope, case.key)
    assert accepted.status_code == 200, accepted.text


def test_deployment_moving_the_gate_refuses_and_unrelated_bundles_deploy(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-2: ``POST /deployments`` rechecks gate ownership first."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    assert register(client, scope, case.key).status_code == 200
    serving = active_deployments(agent)
    for gates in (
        ((LIVE, OTHER_ROUTE),),
        (("Bash", ROUTE),),
        ((LIVE, ROUTE), (LIVE, OTHER_ROUTE)),
    ):
        version = stored_version(client, agent, gates)
        response = deploy(client, agent, version)
        assert response.status_code == 422, (gates, response.text)
        assert active_deployments(agent) == serving
    # Neighbor: a version that keeps the gate and rearranges unrelated gates deploys.
    keep = stored_version(
        client, agent, ((LIVE, ROUTE), ("Bash", ROUTE), ("mcp__other__execute", OTHER_ROUTE)),
    )
    deployed = deploy(client, agent, keep)
    assert deployed.status_code == 201, deployed.text
    # An agent with no registration routes the same tool wherever its bundle says.
    plain = client.post(
        "/agents",
        json={"name": "gate-plain-" + uuid.uuid4().hex[:8],
              "channel": {"kind": "slack", "address": "C0EXAMPLEPLAIN"},
              "approval_routes": {
                  name: binding for name, binding in route_map("C0EXAMPLEPLAIN").items()
                  if name != SEED_ROUTE
              }},
        headers=api_headers(),
    )
    assert plain.status_code == 201, plain.text
    other_version = stored_version(client, plain.json()["id"], ((LIVE, OTHER_ROUTE),))
    other = deploy(client, plain.json()["id"], other_version)
    assert other.status_code == 201, other.text


def _bundleless_version(client: Any, agent_id: str) -> str:
    """A stored version that never received a bundle (``bundle_ref`` is NULL)."""
    created = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v-" + uuid.uuid4().hex[:8], "created_by": "synthetic"},
        headers=api_headers(),
    )
    assert created.status_code == 201, created.text
    assert created.json()["bundle_ref"] is None, created.text
    return str(created.json()["id"])


def _all_deployments(agent_id: str) -> list[dict[str, Any]]:
    """Every deployment row of the agent, active or not, read independently."""
    return sql_dicts(
        "SELECT id, version_id, status FROM curie.deployments WHERE agent_id = :agent "
        "ORDER BY deployed_at, id",
        {"agent": uuid.UUID(agent_id)},
    )


def test_registration_refuses_when_every_active_version_is_bundleless(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-2: a bundleless version declares no gates, so it cannot gate."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope, serve_bundle=False)
    bundleless = _bundleless_version(client, agent)
    deployed = deploy(client, agent, bundleless)
    assert deployed.status_code == 201, deployed.text
    assert len(active_deployments(agent)) == 1
    before = agent_state(agent)
    refused_with(register(client, scope, case.key), 422, MISMATCH)
    after = agent_state(agent)
    assert after["approval_routes"] == before["approval_routes"]
    assert after["version"] == before["version"]
    assert not registered(after)


def test_deploying_a_bundleless_version_refuses_for_a_selector_agent(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-2: a registered pair cannot lose its gate to a bundleless version."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    assert register(client, scope, case.key).status_code == 200
    serving = active_deployments(agent)
    rows = _all_deployments(agent)
    assert len(serving) == 1
    bundleless = _bundleless_version(client, agent)
    refused_with(deploy(client, agent, bundleless), 422, MISMATCH)
    assert _all_deployments(agent) == rows
    assert active_deployments(agent) == serving


def test_bundleless_versions_stay_deployable_without_a_selector(
    operator_case: SimpleNamespace,
) -> None:
    """@spec REQUEST-APPROVAL-2: with no registration a bundleless version gates nothing."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope, serve_bundle=False)
    deployed = deploy(client, agent, _bundleless_version(client, agent))
    assert deployed.status_code == 201, deployed.text
    assert len(active_deployments(agent)) == 1
    # A route write with no selector still succeeds while only a bundleless version serves.
    rewritten = write_routes(
        client, agent, route_map(scope["address"]), expected=scope["config_version"],
    )
    assert rewritten.status_code == 200, rewritten.text
    assert not registered(agent_state(agent))


def test_git_push_route_check_refuses_moving_the_gate(operator_case: SimpleNamespace) -> None:
    """@spec REQUEST-APPROVAL-2: the byte-holding check git flow and uploads share."""
    case = operator_case
    client, scope, agent = case.client, case.scope, case.scope["agent"]
    prepare(client, scope)
    assert register(client, scope, case.key).status_code == 200
    stored = agent_state(agent)["approval_routes"]
    moved, dropped = bundle(((LIVE, OTHER_ROUTE),)), bundle((("Bash", OTHER_ROUTE),))
    for archive in (moved, dropped):
        code = refusal_code(deploy_checks.check_routes_from_bytes(archive, stored, uuid.uuid4()))
        assert code == MISMATCH, code
    assert refusal_code(
        deploy_checks.check_routes_from_bytes(bundle(KEEPS_GATE), stored, uuid.uuid4()),
    ) is None
    unregistered = route_map(scope["address"])
    assert refusal_code(
        deploy_checks.check_routes_from_bytes(moved, unregistered, uuid.uuid4()),
    ) is None


def _bundle_root(root: Path, gates: list[dict[str, Any]] | None) -> Path:
    """A bundle directory whose manifest declares ``gates`` (None: no approvalPolicy)."""
    manifest: dict[str, Any] = {"name": "route-map-plugin", "version": "0.1.0"}
    if gates is not None:
        manifest["approvalPolicy"] = {"gates": gates}
    (root / ".claude-plugin").mkdir(parents=True)
    (root / ".claude-plugin" / "plugin.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


def test_declared_route_map_executes_the_frozen_normalization_vector(tmp_path: Path) -> None:
    """@spec REQUEST-APPROVAL-2: the map reader and the set reader cannot drift.

    The vector is the shared rule the runner's loader also executes; the map's
    values must be exactly the declared set, and its keys are the stripped,
    last-declaration-wins tool names the gate check judges.
    """
    cases = json.loads(VECTOR.read_text(encoding="utf-8"))["cases"]
    assert cases
    for case in cases:
        root = _bundle_root(tmp_path / str(case["id"]), case["gates"])
        mapped = bundles.declared_approval_route_map(root)
        if case["expected"] == "rejected":
            assert mapped is None, case["id"]
            assert bundles.declared_approval_routes(root) is None, case["id"]
            continue
        assert mapped is not None, case["id"]
        assert set(mapped.values()) == set(case["expected"]), case["id"]
        assert set(mapped.values()) == bundles.declared_approval_routes(root), case["id"]
    root = _bundle_root(tmp_path / "last-wins", [
        {"gate": LIVE, "route": OTHER_ROUTE},
        {"gate": f"  {LIVE} ", "route": f" {ROUTE} "},
        {"gate": "Bash", "route": OTHER_ROUTE},
    ])
    assert bundles.declared_approval_route_map(root) == {LIVE: ROUTE, "Bash": OTHER_ROUTE}

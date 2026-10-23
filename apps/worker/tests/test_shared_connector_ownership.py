"""Removing one member removes only that member's reach (ADR 0239, cp5 AC6).

Driven exactly as ``test_shared_connector_apply.py`` drives it: the
production ``ConnectorReconcileLoop.one_pass()`` from
``run._build_connector_loop``, the real API, and the recorded-shape cluster.
Every case shows what must survive explicitly: the other member's reach, the
shared Deployment (same ``metadata.generation``, same template hash), the
Service and policies, the operator Secrets, the worker's key and DSN Secrets,
and the undo history rows.

The set rules under test (plan S2 ``connector_loop.py`` item 4, Edge case 8):
a 200 render or a refusal that names its declarations is authoritative for
reach; a transport error, timeout, 5xx or other exception leaves the agent
unresolved, keeps its reach and blocks application GC; a malformed or
undecidable refusal prunes the agent's reach and blocks GC for the pass.
Application objects are deleted only in a pass with GC unblocked and no
declaring agent. Secrets are never deleted.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import sys
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_shared_connector_apply import (  # noqa: E402
    ALIAS,
    DEDICATED,
    RELEASE,
    Deployed,
    World,
    _required_valkey,
    cleanup,
    dsn_secret_name,
    fail_render,
    key_secret_name,
    logged,
    sessions,
    shared_database,
    template_hash,
    worker_logs,
    world,
)

__all__ = [
    "_required_valkey",
    "cleanup",
    "sessions",
    "shared_database",
    "world",
    "worker_logs",
]


def _survivors(world: World, application_id: str) -> dict[str, Any]:
    """Everything a removal must leave exactly as it was."""

    objects = world.cluster.application_objects(application_id)
    grant_store = world.specs[application_id]["grant_store"]["name"]
    return {
        "objects": {key: obj["metadata"]["resourceVersion"] for key, obj in objects.items()},
        "generation": world.cluster.objects[("Deployment", world.deployment_name(application_id))][
            "metadata"
        ]["generation"],
        "secrets": {
            name: world.cluster.secret(name)
            for name in (
                key_secret_name(application_id),
                dsn_secret_name(application_id),
                grant_store,
            )
        },
    }


def _deleted(world: World) -> list[tuple[str, str]]:
    return [(kind, name) for verb, kind, name in world.last if verb == "delete"]


def _assert_one_template(world: World, application_id: str) -> None:
    sent = world.cluster.sent_named("Deployment", world.deployment_name(application_id))
    assert sent, "the Deployment was applied"
    assert len({template_hash(obj) for obj in sent}) == 1


def _assert_gone(world: World, application_id: str) -> None:
    assert world.cluster.application_objects(application_id) == {}
    expected = {
        (obj["kind"], obj["metadata"]["name"]) for obj in world.expected_application(application_id)
    }
    assert expected <= set(_deleted(world)), _deleted(world)


def _undecidable(response: httpx.Response) -> str:
    """The refusal's code, asserting it names no declarations (r2-5 rule 1.2a).

    A declaration refusal answers ``{"detail": {"code", ..., "declared"}}``
    (S1.E); an installation-level one may keep the plain ``{"detail": code}``
    form. Neither names what the bundle declares.
    """

    detail = response.json()["detail"]
    if isinstance(detail, str):
        return detail
    assert detail["declared"] is None, detail
    return str(detail["code"])


def _two_members(world: World) -> tuple[str, Deployed, Deployed]:
    application_id = world.application()
    first, second = world.member(application_id), world.member(application_id)
    world.converge(application_id)
    assert world.cluster.reach(first.agent_id) and world.cluster.reach(second.agent_id)
    return application_id, first, second


def test_remove_one_member_prunes_only_its_reach(world: World) -> None:
    application_id, first, second = _two_members(world)
    kept_reach = world.cluster.reach(first.agent_id)
    removed_reach = set(world.cluster.reach(second.agent_id))
    survivors = _survivors(world, application_id)

    world.remove_member(application_id, second.agent_id)
    world.run_pass()

    assert world.cluster.reach(second.agent_id) == {}
    assert {name for kind, name in _deleted(world)} == removed_reach
    assert world.cluster.reach(first.agent_id) == kept_reach
    assert _survivors(world, application_id) == survivors
    _assert_one_template(world, application_id)


def test_remove_one_bundle_declaration_prunes_only_its_reach(world: World) -> None:
    application_id, first, second = _two_members(world)
    kept_reach = world.cluster.reach(first.agent_id)
    removed_reach = set(world.cluster.reach(second.agent_id))
    survivors = _survivors(world, application_id)

    world.deploy(second, shared={})
    world.run_pass()

    assert world.cluster.reach(second.agent_id) == {}
    assert {name for kind, name in _deleted(world)} == removed_reach
    assert world.cluster.reach(first.agent_id) == kept_reach
    assert _survivors(world, application_id) == survivors
    _assert_one_template(world, application_id)


def test_last_reference_removed_keeps_secrets(world: World) -> None:
    application_id, first, second = _two_members(world)
    kept_secrets = _survivors(world, application_id)["secrets"]

    world.deploy(first, shared={})
    world.deploy(second, shared={})
    world.run_pass()

    _assert_gone(world, application_id)
    assert world.cluster.reach() == {}
    assert _survivors_secrets(world, application_id) == kept_secrets
    assert [kind for kind, _ in _deleted(world) if kind == "Secret"] == []


def _survivors_secrets(world: World, application_id: str) -> dict[str, Any]:
    grant_store = world.specs[application_id]["grant_store"]["name"]
    return {
        name: world.cluster.secret(name)
        for name in (key_secret_name(application_id), dsn_secret_name(application_id), grant_store)
    }


def _unreachable(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(503, json={"detail": "upstream unavailable"})


def test_transport_error_blocks_application_gc(world: World) -> None:
    application_id = world.application()
    declaring = world.member(application_id)
    bystander = world.deploy(world.agent(), shared={})
    world.converge(application_id)
    world.deploy(declaring, shared={})
    rule = fail_render(bystander.agent_id, _unreachable)
    world.transport.rules.append(rule)

    world.run_pass()

    assert world.cluster.application_objects(application_id), "GC ran with an agent unresolved"
    assert not [name for kind, name in _deleted(world) if kind == "Deployment"]
    # The declaring agent answered authoritatively: its reach is pruned anyway.
    assert world.cluster.reach(declaring.agent_id) == {}

    world.transport.rules.remove(rule)
    world.run_pass()
    _assert_gone(world, application_id)


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(lambda request: httpx.ConnectError("refused", request=request), id="connect"),
        pytest.param(lambda request: httpx.ReadTimeout("timed out", request=request), id="timeout"),
    ],
)
def test_unresolved_agent_keeps_reach_across_passes(
    world: World, error: Callable[[httpx.Request], BaseException]
) -> None:
    application_id, unresolved, converging = _two_members(world)
    kept = world.cluster.reach(unresolved.agent_id)
    kept_names = set(kept)
    world.transport.rules.append(fail_render(unresolved.agent_id, error))
    world.remove_member(application_id, converging.agent_id)

    for number in range(4):  # past the 3-pass 5xx grace
        summary = world.run_pass()
        assert world.cluster.reach(unresolved.agent_id) == kept, number
        assert kept_names.isdisjoint(name for _, name in _deleted(world)), number
        assert summary.shared_unresolved == 1, number
        # The other member converges normally in the very first pass.
        assert world.cluster.reach(converging.agent_id) == {}, number
        assert world.cluster.application_objects(application_id), number


def test_disabled_application_keeps_deployment_prunes_all_reach(world: World) -> None:
    application_id, first, second = _two_members(world)
    survivors = _survivors(world, application_id)
    world.disable(application_id)

    for number in range(2):
        world.run_pass()
        assert world.cluster.reach() == {}, number
        assert _survivors(world, application_id) == survivors, number
        assert not [kind for kind, _ in _deleted(world) if kind in ("Deployment", "Service")]


def test_gc_only_after_last_declaration_removed(world: World) -> None:
    application_id, first, second = _two_members(world)
    survivors = _survivors(world, application_id)

    # A refused render that names the application still declares it.
    world.remove_member(application_id, first.agent_id)
    world.run_pass()
    assert world.cluster.reach(first.agent_id) == {}
    assert _survivors(world, application_id) == survivors

    # The first agent's declaration is gone; the second still declares it.
    world.deploy(first, shared={})
    world.run_pass()
    assert _survivors(world, application_id) == survivors
    assert world.cluster.reach(second.agent_id)

    world.deploy(second, shared={})
    world.run_pass()
    _assert_gone(world, application_id)
    assert _survivors_secrets(world, application_id) == survivors["secrets"]


MALFORMED = (
    "connectors:\n"
    f"  {ALIAS}:\n"
    "    shared:\n"
    "      application: {application}\n"
    "      contract: 1\n"
    "    image: example.com/acme/mixed@sha256:" + "e" * 64 + "\n"
)


@pytest.mark.parametrize("variant", ["malformed-bundle", "installation-refusal", "no-worker-token"])
def test_malformed_refusal_prunes_reach_but_blocks_gc(
    world: World,
    worker_logs: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    variant: str,
) -> None:
    from curie_api.config import get_settings
    from plugin_format.connector_render import object_name

    application_id, undecidable, other = _two_members(world)
    # The other member stops declaring first, so no authoritative agent
    # declares the application: only the undecidable answer can block GC.
    world.deploy(other, shared={})
    world.run_pass()
    assert world.cluster.application_objects(application_id)
    assert world.cluster.reach(undecidable.agent_id)
    survivors = _survivors(world, application_id)
    loop = None
    if variant == "malformed-bundle":
        broken = world.store_raw(undecidable, MALFORMED.format(application=application_id))
        response = world.render(broken)
        assert response.status_code == 422, response.text
        assert _undecidable(response) == "connectors.shared_invalid"
    elif variant == "installation-refusal":
        monkeypatch.setenv("CURIE_INSTALLATION_ID_OBSERVED", "false")
        get_settings.cache_clear()
        response = world.render(undecidable)
        assert response.status_code == 409, response.text
        assert _undecidable(response) == "shared_installation_unobserved"
    else:
        loop = world.build_loop(internal_worker_token="")
        dedicated_name = object_name(RELEASE, other.name, DEDICATED)
        world.drop("Deployment", dedicated_name)

    world.run_pass(loop)

    assert world.cluster.reach(undecidable.agent_id) == {}
    assert _survivors(world, application_id) == survivors, "GC ran on an undecidable pass"
    if variant == "no-worker-token":
        assert logged(worker_logs, "shared_worker_token_missing")
        # Dedicated agents are unaffected by the missing token.
        assert ("Deployment", dedicated_name) in world.cluster.objects

    # Liveness: once the agent's answer is decidable again, the next pass GCs.
    if variant == "installation-refusal":
        monkeypatch.setenv("CURIE_INSTALLATION_ID_OBSERVED", "true")
        get_settings.cache_clear()
    world.deploy(undecidable, shared={})
    world.run_pass()
    _assert_gone(world, application_id)
    assert _survivors_secrets(world, application_id) == survivors["secrets"]


def _undo_history(agent_id: str) -> tuple[int, str]:
    from _migration_support import sql_dicts

    rows = sql_dicts(
        "SELECT 'a' || t::text AS t FROM curie.agent_actions t WHERE agent_id = :agent "
        "UNION ALL "
        "SELECT 'e' || t::text AS t FROM curie.action_executions t WHERE agent_id = :agent",
        {"agent": uuid.UUID(agent_id)},
    )
    texts = sorted(row["t"] for row in rows)
    return len(texts), hashlib.sha256("\n".join(texts).encode()).hexdigest()


def _seed_undo_history(agent_id: str) -> None:
    from _migration_support import sql_rows

    action_id = uuid.uuid4()
    sealed = {
        "sealed": "curie.snapshot.v1",
        "kid": "snapshot-key-1",
        "ciphertext": base64.b64encode(secrets.token_bytes(24)).decode("ascii"),
    }
    sql_rows(
        "INSERT INTO curie.agent_actions "
        "(id, agent_id, conversation_id, call_id, tool, arguments, prior_state, target, "
        "status, dedupe_key, completed_at, post_version, connector, connector_digest) "
        "VALUES (:id, :agent, 'C1', :call, 'mcp__shared-a__update', CAST(:arguments AS jsonb), "
        "CAST(:prior AS jsonb), CAST(:target AS jsonb), 'succeeded', :key, now(), 'v2', "
        ":connector, :digest)",
        {
            "id": action_id,
            "agent": uuid.UUID(agent_id),
            "call": f"call-{uuid.uuid4().hex[:8]}",
            "arguments": json.dumps({"id": "42"}),
            "prior": json.dumps(sealed),
            "target": json.dumps({"id": "42"}),
            "key": f"event-{uuid.uuid4()}",
            "connector": DEDICATED,
            "digest": "sha256:" + "d" * 64,
        },
    )
    execution_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.action_executions "
        "(id, kind, agent_id, connector, tool, subject_action_id, connector_digest, "
        "authority_kind, authority_ref, requested_by, idempotency_key, state, attempt, "
        "created_at) "
        "VALUES (:id, 'restore', :agent, :connector, 'restore', :subject, :digest, "
        "'undo_ruling', :ref, 'U-operator', :key, 'confirmed', 1, now())",
        {
            "id": execution_id,
            "agent": uuid.UUID(agent_id),
            "connector": DEDICATED,
            "subject": action_id,
            "digest": "sha256:" + "d" * 64,
            "ref": str(uuid.uuid4()),
            "key": f"restore:{action_id}:{execution_id}",
        },
    )


def test_undo_history_preserved(world: World) -> None:
    application_id, first, second = _two_members(world)
    _seed_undo_history(second.agent_id)
    before = _undo_history(second.agent_id)
    assert before[0] == 2

    world.remove_member(application_id, second.agent_id)
    world.run_pass()
    world.deploy(second, shared={})
    world.run_pass()

    assert world.cluster.reach(second.agent_id) == {}
    assert _undo_history(second.agent_id) == before
    # Liveness of the observation: the other member is still served.
    assert world.cluster.reach(first.agent_id)


def test_undeployed_agent_reach_swept(world: World) -> None:
    from _migration_support import sql_rows

    application_id, first, second = _two_members(world)
    kept = world.cluster.reach(second.agent_id)
    survivors = _survivors(world, application_id)
    sql_rows(
        "UPDATE curie.deployments SET status = 'inactive' WHERE agent_id = :agent",
        {"agent": uuid.UUID(first.agent_id)},
    )

    world.run_pass()

    assert world.cluster.reach(first.agent_id) == {}
    assert world.cluster.reach(second.agent_id) == kept
    assert _survivors(world, application_id) == survivors

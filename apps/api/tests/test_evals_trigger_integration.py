"""On-demand eval trigger endpoint (issue #10) against real Postgres + Valkey.

Asserts POST /evals/trigger resolves an agent's active dev deployment (or an
explicit version) and enqueues the SAME EvalJob shape onto curie:evals
that the git-push fan-out uses -- minus the push-only gate. Mirrors
test_evalqueue_integration's stream assertions and the router auth tests.
"""

import asyncio
import json
import uuid
from typing import Any

import redis
from aci_protocol import STREAM_PAYLOAD_FIELD, EvalJob
from curie_api import crud
from curie_api.config import get_settings
from curie_api.models import Environment
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

REPO = "octo/trigger-10"


def _create_agent(client: Any, auth_headers: dict[str, str], name: str) -> dict[str, Any]:
    resp = client.post(
        "/agents",
        json={"name": name, "slack_channel": "C000000K01", "repo_full_name": REPO},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _seed_version_and_dev_deployment(
    agent_id: str,
    *,
    version_label: str,
    commit_sha: str | None,
    bundle_ref: str | None,
    deploy: bool = True,
) -> dict[str, str | None]:
    """Insert a version (with commit_sha/bundle_ref) and, optionally, an active
    dev deployment for it -- straight through the crud layer against the same
    disposable DB the app is bound to (mirrors conftest._truncate)."""

    engine = create_async_engine(get_settings().database_url)
    sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessionmaker() as session:
            version = await crud.create_version_row(
                session,
                uuid.UUID(agent_id),
                version_label=version_label,
                created_by="test",
                commit_sha=commit_sha,
                bundle_ref=bundle_ref,
            )
            version_id = str(version.id)
            if deploy:
                await crud.create_deployment_row(
                    session,
                    uuid.UUID(agent_id),
                    version.id,
                    Environment.dev,
                    commit_sha=commit_sha,
                )
    finally:
        await engine.dispose()
    return {"version_id": version_id, "sha": commit_sha, "bundle_ref": bundle_ref}


def _seed(
    agent_id: str,
    *,
    version_label: str = "v1",
    commit_sha: str | None = "cafef00d",
    bundle_ref: str | None = "bundles/x/y.tar.gz",
    deploy: bool = True,
) -> dict[str, str | None]:
    return asyncio.run(
        _seed_version_and_dev_deployment(
            agent_id,
            version_label=version_label,
            commit_sha=commit_sha,
            bundle_ref=bundle_ref,
            deploy=deploy,
        )
    )


def _payload_for_stream_id(stream_id: str) -> dict[str, Any] | None:
    sync = redis.from_url(get_settings().valkey_dsn())
    try:
        entries = sync.xrange("curie:evals", min=stream_id, max=stream_id)
        if not entries:
            return None
        _id, fields = entries[0]
        return json.loads(fields[STREAM_PAYLOAD_FIELD.encode()])
    finally:
        sync.close()


def _count_eval_entries_for_agent(agent_id: str) -> int:
    sync = redis.from_url(get_settings().valkey_dsn())
    try:
        return sum(
            1
            for _id, fields in sync.xrevrange("curie:evals", count=200)
            if json.loads(fields[STREAM_PAYLOAD_FIELD.encode()]).get("agent_id") == agent_id
        )
    finally:
        sync.close()


def test_trigger_enqueues_for_active_dev_deployment(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, "trigger-active")
    seeded = _seed(agent["id"])

    resp = client.post("/evals/trigger", json={"agent_id": agent["id"]}, headers=auth_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["stream_id"]
    assert body["version_id"] == seeded["version_id"]
    assert body["sha"] == seeded["sha"]
    assert body["suite"] == get_settings().eval_default_suite
    assert body["bundle_ref"] == seeded["bundle_ref"]

    # The decoded stream payload round-trips through EvalJob with exactly
    # the resolved fields (same wire contract as the git-push fan-out).
    payload = _payload_for_stream_id(body["stream_id"])
    assert payload is not None
    req = EvalJob.model_validate(payload)
    assert str(req.agent_id) == agent["id"]
    assert str(req.version_id) == seeded["version_id"]
    assert req.sha == seeded["sha"]
    assert req.suite == get_settings().eval_default_suite
    assert req.bundle_ref == seeded["bundle_ref"]
    assert req.target_url is None
    assert req.model is None  # no model requested -> worker default


def test_trigger_threads_requested_model_onto_the_job(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """#526: a requested model is echoed in the result AND carried on the enqueued
    EvalJob, so the worker boots+tags that model and a sweep's rows land in
    the matrix's model column (one trigger per model)."""
    agent = _create_agent(client, auth_headers, "trigger-model")
    _seed(agent["id"])

    resp = client.post(
        "/evals/trigger",
        json={"agent_id": agent["id"], "model": "claude-sweep-x"},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["model"] == "claude-sweep-x"

    payload = _payload_for_stream_id(resp.json()["stream_id"])
    assert payload is not None
    assert EvalJob.model_validate(payload).model == "claude-sweep-x"


def test_trigger_requires_api_key(client: Any) -> None:
    assert client.post("/evals/trigger", json={"agent_id": str(uuid.uuid4())}).status_code == 401


def test_trigger_unknown_agent_returns_404_and_does_not_enqueue(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    missing = str(uuid.uuid4())
    resp = client.post("/evals/trigger", json={"agent_id": missing}, headers=auth_headers)
    assert resp.status_code == 404, resp.text
    assert _count_eval_entries_for_agent(missing) == 0


def test_trigger_agent_without_active_dev_deployment_returns_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, "trigger-nodeploy")
    # A version exists but was never deployed to dev.
    _seed(agent["id"], deploy=False)
    resp = client.post("/evals/trigger", json={"agent_id": agent["id"]}, headers=auth_headers)
    assert resp.status_code == 404, resp.text
    assert _count_eval_entries_for_agent(agent["id"]) == 0


def test_trigger_suite_defaults_and_explicit(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, "trigger-suite")
    _seed(agent["id"])

    # Omitted -> eval_default_suite.
    default_resp = client.post(
        "/evals/trigger", json={"agent_id": agent["id"]}, headers=auth_headers
    )
    assert default_resp.status_code == 200, default_resp.text
    assert default_resp.json()["suite"] == get_settings().eval_default_suite

    # Explicit -> honored, and rides onto the stream.
    explicit = client.post(
        "/evals/trigger",
        json={"agent_id": agent["id"], "suite": "regression"},
        headers=auth_headers,
    )
    assert explicit.status_code == 200, explicit.text
    assert explicit.json()["suite"] == "regression"
    payload = _payload_for_stream_id(explicit.json()["stream_id"])
    assert payload is not None and payload["suite"] == "regression"


def test_trigger_explicit_version_id_is_used(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, "trigger-explicit-version")
    # Active dev deployment points at v1; the caller asks for v2 explicitly.
    _seed(agent["id"], version_label="v1", commit_sha="1111aaaa", bundle_ref="b/1.tgz")
    v2 = _seed(
        agent["id"],
        version_label="v2",
        commit_sha="2222bbbb",
        bundle_ref="b/2.tgz",
        deploy=False,
    )

    resp = client.post(
        "/evals/trigger",
        json={"agent_id": agent["id"], "version_id": v2["version_id"]},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["version_id"] == v2["version_id"]
    assert body["sha"] == "2222bbbb"
    assert body["bundle_ref"] == "b/2.tgz"


def test_trigger_version_without_bundle_returns_400_and_does_not_enqueue(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, "trigger-no-bundle")
    # The version has a commit sha but its bundle was never built; the worker
    # reads eval cases from the bundle, so there is nothing to evaluate.
    seeded = _seed(agent["id"], commit_sha="deadbeef", bundle_ref=None)

    resp = client.post(
        "/evals/trigger",
        json={"agent_id": agent["id"], "version_id": seeded["version_id"]},
        headers=auth_headers,
    )
    assert resp.status_code == 400, resp.text
    assert "no built bundle" in resp.json()["detail"]
    assert _count_eval_entries_for_agent(agent["id"]) == 0


def test_trigger_active_dev_deployment_without_bundle_returns_400(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # Same guard on the active-dev-deployment resolution path (no version_id).
    agent = _create_agent(client, auth_headers, "trigger-no-bundle-deploy")
    _seed(agent["id"], commit_sha="deadbeef", bundle_ref=None)

    resp = client.post("/evals/trigger", json={"agent_id": agent["id"]}, headers=auth_headers)
    assert resp.status_code == 400, resp.text
    assert "no built bundle" in resp.json()["detail"]
    assert _count_eval_entries_for_agent(agent["id"]) == 0


def test_trigger_unknown_version_for_agent_returns_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent = _create_agent(client, auth_headers, "trigger-bad-version")
    _seed(agent["id"])
    resp = client.post(
        "/evals/trigger",
        json={"agent_id": agent["id"], "version_id": str(uuid.uuid4())},
        headers=auth_headers,
    )
    assert resp.status_code == 404, resp.text
    assert _count_eval_entries_for_agent(agent["id"]) == 0

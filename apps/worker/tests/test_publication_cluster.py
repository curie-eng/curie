"""Real cluster proof for the hardened publication Job contract."""

from __future__ import annotations

import asyncio
import base64
import copy
import dataclasses
import hashlib
import json
import os
import ssl
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import pytest
from channel_protocol.reply import ReplyAck
from curie_worker.publication_clients import PublicationCodeHostClient, PublicationLineageClient
from curie_worker.publication_k8s import (
    KubernetesPublicationCluster,
    PublicationJobSettings,
    PublicationPayload,
    PublicationResourceError,
    PublicationTransport,
    build_publication_resources,
    job_transport,
    validate_adopted_resource,
)
from curie_worker.publication_loop import (
    PublicationCredential,
    PublicationJobObservation,
    PublicationPullState,
    PublicationReconciler,
)
from curie_worker.publication_store import PostgresPublicationStore
from kubernetes import client as k8s_client
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

OPT_IN = os.environ.get("CURIE_PUBLICATION_CLUSTER_PROOF") == "1"
pytestmark = [pytest.mark.anyio]
# The cluster proofs need the owned kind wrapper; the builder contract checks
# at the end of this module run anywhere.
requires_cluster = pytest.mark.skipif(
    not OPT_IN,
    reason="set CURIE_PUBLICATION_CLUSTER_PROOF=1 through the owned cluster wrapper",
)

REPO_FULL_NAME = "acme-corp/acme-bot"
OWNER_NAME = "publication-owner"
SERVICE_ACCOUNT = "publication-runner"
AUTHORIZATION = "Bearer fixture-token"
REST_AUTHORIZATION = "Basic " + base64.b64encode(b"x-access-token:fixture-token").decode()
ADOPT_ID = uuid.UUID("20000001-2222-4222-8222-000000000001")
MISMATCH_ID = uuid.UUID("20000002-2222-4222-8222-000000000002")
FAILURE_ID = uuid.UUID("20000003-2222-4222-8222-000000000003")
POSITIVE_ID = uuid.UUID("20000004-2222-4222-8222-000000000004")
TRANSPORT_ID = uuid.UUID("20000005-2222-4222-8222-000000000005")
UPGRADE_ID = uuid.UUID("20000006-2222-4222-8222-000000000006")
AGENT_ID = uuid.UUID("11111111-1111-4111-8111-000000000003")
VERSION_ID = uuid.UUID("33333333-3333-4333-8333-000000000003")
DEPLOYMENT_ID = uuid.UUID("44444444-4444-4444-8444-000000000003")
APPROVAL_ID = uuid.UUID("55555555-5555-4555-8555-000000000003")
LINEAGE_ID = uuid.UUID("66666666-6666-4666-8666-000000000003")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.fail(f"enabled publication cluster proof is missing {name}")
    return value


def _namespace() -> str:
    return _required("CURIE_PUBLICATION_NAMESPACE")


def _origin() -> str:
    return _required("CURIE_PUBLICATION_FIXTURE_CLUSTER_API")


def _transport() -> PublicationTransport:
    # The fixture CA is baked into the fixture runner image's system trust
    # store, so the Job needs no mounted code host CA bundle.
    return PublicationTransport(origin=_origin(), header_form="authorization_bearer")


def _cluster() -> KubernetesPublicationCluster:
    return KubernetesPublicationCluster(
        _namespace(),
        kubeconfig=_required("CURIE_PUBLICATION_KUBECONFIG"),
    )


def _settings() -> PublicationJobSettings:
    return PublicationJobSettings(
        namespace=_namespace(),
        runner_image=_required("CURIE_PUBLICATION_RUNNER_IMAGE"),
        image_pull_policy="Never",
        image_pull_secrets=(),
        priority_class_name="",
        service_account_name=SERVICE_ACCOUNT,
        owner_name=OWNER_NAME,
        git_user_name="Curie Publisher",
        git_user_email="publisher@example.com",
        cpu_request="25m",
        cpu_limit="500m",
        memory_request="64Mi",
        memory_limit="512Mi",
        ephemeral_request="128Mi",
        ephemeral_limit="1Gi",
        active_deadline_seconds=90,
        git_timeout_seconds=20,
    )


def _payload(
    publication_id: uuid.UUID,
    *,
    base_sha: str,
    patch: bytes,
    transport: PublicationTransport | None = None,
) -> PublicationPayload:
    transport = transport or _transport()
    return PublicationPayload(
        publication_id=publication_id,
        revision_id=publication_id,
        revision_number=1,
        repo_full_name=REPO_FULL_NAME,
        clean_clone_url=f"{transport.origin}/{REPO_FULL_NAME}.git",
        base_sha=base_sha,
        expected_prior_head=base_sha,
        expected_remote_head=None,
        patch=patch,
        branch=f"curie/publication-{publication_id.hex}",
        title="Publish cluster proof",
        transport=transport,
    )


def _ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context(cafile=_required("CURIE_PUBLICATION_FIXTURE_CA"))


async def _forward_fixture_request(request: httpx.Request) -> None:
    """Reach the fixture port forward while retaining its configured forge identity."""
    if str(request.url).startswith(_required("CURIE_PUBLICATION_FIXTURE_CLUSTER_API") + "/"):
        forwarded = httpx.URL(_required("CURIE_PUBLICATION_FIXTURE_API"))
        request.url = request.url.copy_with(
            scheme=forwarded.scheme, host=forwarded.host, port=forwarded.port
        )


async def _fixture_get(path: str) -> httpx.Response:
    authorization = REST_AUTHORIZATION if path.startswith("/repos/") else AUTHORIZATION
    async with httpx.AsyncClient(verify=_ssl_context(), timeout=10) as client:
        response = await client.get(
            f"{_required('CURIE_PUBLICATION_FIXTURE_API')}{path}",
            headers={"Authorization": authorization},
        )
    return response


async def _base_sha() -> str:
    response = await _fixture_get("/__fixture/state")
    assert response.status_code == 200, response.text
    value = response.json()["base_sha"]
    assert isinstance(value, str) and len(value) == 40
    return value


async def _wait_terminal(
    cluster: KubernetesPublicationCluster,
    job_name: str,
    *,
    timeout_seconds: float = 90,
) -> PublicationJobObservation:
    deadline = time.monotonic() + timeout_seconds
    last: PublicationJobObservation | None = None
    while time.monotonic() < deadline:
        last = cluster.observe(job_name)
        if last.phase in {"succeeded", "failed"}:
            return last
        await asyncio.sleep(0.5)
    pytest.fail(f"publication Job {job_name} did not terminate; last observation={last!r}")


def _live_objects(
    cluster: KubernetesPublicationCluster,
    resources: Any,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    live_uid = cluster.owner_uid(OWNER_NAME)
    config_map = copy.deepcopy(resources.config_map)
    secret = copy.deepcopy(resources.secret)
    job = copy.deepcopy(resources.job)
    for item in (config_map, secret, job):
        item["metadata"]["ownerReferences"][0]["uid"] = live_uid
    return config_map, secret, job


@requires_cluster
async def test_fixture_rest_and_control_routes_require_their_exact_auth_contracts() -> None:
    # Observed 2026-10-22: a public GitHub repository's exact GET
    # /repos/{owner}/{repo}/git/ref/heads/main with Basic x-access-token returned
    # 200 and authenticated X-RateLimit-Limit=5000. Replay this consumer's
    # exact Basic contract; this does not claim GitHub rejects Bearer generally.
    base_sha = await _base_sha()
    branch = await _fixture_get(f"/repos/{REPO_FULL_NAME}/git/ref/heads/main")
    assert branch.status_code == 200, branch.text
    assert branch.json() == {"ref": "refs/heads/main", "object": {"sha": base_sha}}

    origin = _required("CURIE_PUBLICATION_FIXTURE_API")
    async with httpx.AsyncClient(verify=_ssl_context(), timeout=10) as client:
        for authorization in (
            None,
            "Basic " + base64.b64encode(b"x-access-token:wrong-token").decode(),
            "Basic " + base64.b64encode(b"wrong-user:fixture-token").decode(),
            AUTHORIZATION,
            "Bearer wrong-token",
        ):
            response = await client.get(
                f"{origin}/repos/{REPO_FULL_NAME}/git/ref/heads/main",
                headers={"Authorization": authorization} if authorization is not None else {},
            )
            assert response.status_code == 401, response.text

        for authorization in (None, REST_AUTHORIZATION, "Bearer wrong-token"):
            response = await client.get(
                f"{origin}/__fixture/state",
                headers={"Authorization": authorization} if authorization is not None else {},
            )
            assert response.status_code == 401, response.text


@requires_cluster
async def test_first_publication_is_created_then_adopted_after_api_normalization() -> None:
    cluster = _cluster()
    resources = build_publication_resources(
        _payload(ADOPT_ID, base_sha=await _base_sha(), patch=b"not a patch\n"),
        credential=AUTHORIZATION,
        settings=_settings(),
    )
    container = resources.job["spec"]["template"]["spec"]["containers"][0]
    env = {item["name"]: item["value"] for item in container["env"]}
    assert env["EXPECTED_REMOTE_HEAD"] == ""
    assert not any(name.startswith(("PR_", "GITHUB_")) for name in env)
    assert resources.job["spec"]["template"]["spec"]["imagePullSecrets"] == []
    assert resources.job["spec"]["template"]["spec"]["priorityClassName"] == ""

    cluster.apply(resources)
    api = k8s_client.ApiClient()
    observed = api.sanitize_for_serialization(
        cluster._batch.read_namespaced_job(resources.names.job, _namespace())
    )
    pod_spec = observed["spec"]["template"]["spec"]
    assert pod_spec.get("imagePullSecrets") is None
    assert pod_spec.get("priorityClassName") is None
    observed_env = {item["name"]: item.get("value") for item in pod_spec["containers"][0]["env"]}
    assert observed_env["EXPECTED_REMOTE_HEAD"] is None
    assert observed_env["PUBLICATION_BRANCH_PREFIX"] is None

    cluster.apply(resources)
    cluster.validate_existing(resources)
    assert cluster._core.read_namespaced_config_map(
        resources.names.config_map, _namespace()
    ).metadata.uid
    assert cluster._core.read_namespaced_secret(resources.names.secret, _namespace()).metadata.uid
    assert cluster._batch.read_namespaced_job(resources.names.job, _namespace()).metadata.uid


@requires_cluster
async def test_success_marker_cannot_authorize_a_mismatched_job() -> None:
    cluster = _cluster()
    resources = build_publication_resources(
        _payload(MISMATCH_ID, base_sha=await _base_sha(), patch=b"not a patch\n"),
        credential=AUTHORIZATION,
        settings=_settings(),
    )
    config_map, secret, planted_job = _live_objects(cluster, resources)
    planted_job["spec"]["template"]["spec"]["containers"][0]["command"] = [
        "/bin/bash",
        "-c",
        f"printf '%s\\n' 'CURIE_COMMIT_SHA={'b' * 40}'",
    ]
    cluster._core.create_namespaced_config_map(_namespace(), body=config_map)
    cluster._core.create_namespaced_secret(_namespace(), body=secret)
    cluster._batch.create_namespaced_job(_namespace(), body=planted_job)

    observation = await _wait_terminal(cluster, resources.names.job)
    assert observation.phase == "succeeded"
    assert observation.commit_sha == "b" * 40
    # The planted Job kept the builder's env, so its transport reads back
    # intact; only the full contract check stops the marker from counting.
    assert observation.transport == _transport()

    with pytest.raises(
        PublicationResourceError,
        match=rf"refusing to adopt Job {resources.names.job!r}: spec mismatch",
    ):
        cluster.apply(resources)
    with pytest.raises(
        PublicationResourceError,
        match=rf"refusing to adopt Job {resources.names.job!r}: spec mismatch",
    ):
        cluster.validate_existing(resources)


@requires_cluster
async def test_observe_returns_the_transport_the_builder_wrote_and_refuses_another() -> None:
    cluster = _cluster()
    base_sha = await _base_sha()
    written = _payload(TRANSPORT_ID, base_sha=base_sha, patch=b"not a patch\n")
    resources = build_publication_resources(written, credential=AUTHORIZATION, settings=_settings())
    cluster.apply(resources)

    # Read back through the apiserver, after its normalization of the Job.
    observation = cluster.observe(resources.names.job)
    assert observation.exists
    assert observation.transport == written.transport
    cluster.validate_existing(resources)

    for other in (
        dataclasses.replace(written.transport, header_form="private_token"),
        dataclasses.replace(written.transport, origin=f"{_origin()}/elsewhere"),
    ):
        mismatched = build_publication_resources(
            _payload(TRANSPORT_ID, base_sha=base_sha, patch=b"not a patch\n", transport=other),
            credential=AUTHORIZATION,
            settings=_settings(),
        )
        with pytest.raises(PublicationResourceError, match=r"refusing to adopt "):
            cluster.validate_existing(mismatched)
        with pytest.raises(PublicationResourceError, match=r"refusing to adopt "):
            cluster.apply(mismatched)


class _Credentials:
    def __init__(self) -> None:
        self.calls: list[uuid.UUID] = []

    async def redeem(self, publication_id: uuid.UUID) -> PublicationCredential:
        self.calls.append(publication_id)
        transport = _transport()
        return PublicationCredential(
            clean_clone_url=f"{transport.origin}/{REPO_FULL_NAME}.git",
            authorization_header=AUTHORIZATION,
            origin=transport.origin,
            header_form=transport.header_form,
            ca_bundle_ref=transport.ca_bundle_ref,
        )


class _Replies:
    def __init__(self) -> None:
        self.texts: list[str] = []

    async def emit(self, event: Any, **_kwargs: Any) -> ReplyAck:
        text_value = getattr(event, "text", None)
        if isinstance(text_value, str):
            self.texts.append(text_value)
        return ReplyAck()


class _Transcript:
    def __init__(self) -> None:
        self.texts: list[str] = []

    async def record_result(
        self,
        _agent_id: uuid.UUID,
        _conversation_id: str,
        _publication_id: uuid.UUID,
        text_value: str,
    ) -> None:
        self.texts.append(text_value)


async def _seed_publication(
    engine: AsyncEngine,
    base_sha: str,
    *,
    publication_id: uuid.UUID = FAILURE_ID,
    patch: bytes = b"this is not a unified diff\n",
) -> None:
    branch = f"curie/publication-{publication_id.hex}"
    agent_id = uuid.uuid5(publication_id, "agent")
    version_id = uuid.uuid5(publication_id, "version")
    deployment_id = uuid.uuid5(publication_id, "deployment")
    approval_id = uuid.uuid5(publication_id, "approval")
    lineage_id = uuid.uuid5(publication_id, "lineage")
    async with engine.begin() as connection:
        await connection.execute(
            text("INSERT INTO curie.agents (id, name, repo_full_name) VALUES (:id, :name, :repo)"),
            {
                "id": agent_id,
                "name": f"acme-publication-{publication_id.hex}",
                "repo": REPO_FULL_NAME,
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.agent_versions "
                "(id, agent_id, version_label, created_by) "
                "VALUES (:id, :agent_id, 'cluster-proof', 'fixture')"
            ),
            {"id": version_id, "agent_id": agent_id},
        )
        await connection.execute(
            text(
                "INSERT INTO curie.deployments "
                "(id, agent_id, version_id, environment, workspace_enabled, status) "
                "VALUES (:id, :agent_id, :version_id, 'dev', true, 'active')"
            ),
            {
                "id": deployment_id,
                "agent_id": agent_id,
                "version_id": version_id,
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.approvals "
                "(id, agent_id, conversation_id, author, summary, reply_kind, "
                "reply_channel, reply_placeholder, dedupe_key, status, resolved_by, "
                "resolution_note, purpose) VALUES "
                "(:id, :agent_id, '1700000000.000100', 'requester@example.test', "
                "'Publish these repository changes?', 'slack', 'C0EXAMPLE1', "
                "'1700000000.000200', :dedupe_key, 'approved', "
                "'U0APPROVE1', 'Approved fixture publication.', 'publication')"
            ),
            {
                "id": approval_id,
                "agent_id": agent_id,
                "dedupe_key": f"publication-cluster-proof-{publication_id}",
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.thread_publication_lineages "
                "(id, agent_id, deployment_id, conversation_id, repo_full_name, "
                "base_sha, branch, status, version, latest_revision) VALUES "
                "(:id, :agent_id, :deployment_id, 'slack:C0EXAMPLE1:1700000000.000100', "
                ":repo, :base_sha, :branch, 'open', 1, 1)"
            ),
            {
                "id": lineage_id,
                "agent_id": agent_id,
                "deployment_id": deployment_id,
                "repo": REPO_FULL_NAME,
                "base_sha": base_sha,
                "branch": branch,
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.publications "
                "(id, approval_id, deployment_id, workspace_conversation_id, "
                "lineage_id, revision_number, expected_prior_head, repo_full_name, "
                "status, base_sha, patch_bytes, changed_paths, title, body, reply_kind, "
                "reply_channel, reply_placeholder, approval_card_reported_at) VALUES "
                "(:id, :approval_id, :deployment_id, "
                "'slack:C0EXAMPLE1:1700000000.000100', :lineage_id, 1, :base_sha, "
                ":repo, 'approved', :base_sha, :patch, "
                "CAST(:changed_paths AS jsonb), 'Publish cluster proof', "
                "'Approved publication cluster proof.', 'slack', 'C0EXAMPLE1', "
                "'1700000000.000200', now())"
            ),
            {
                "id": publication_id,
                "approval_id": approval_id,
                "deployment_id": deployment_id,
                "lineage_id": lineage_id,
                "base_sha": base_sha,
                "repo": REPO_FULL_NAME,
                "patch": patch,
                "changed_paths": '["README.md"]',
            },
        )


class _NoBranchCodeHost:
    """The API's code host routes for a publication whose branch was never pushed."""

    async def read_branch_head(self, publication_id: uuid.UUID) -> str | None:
        return None

    async def read_pull_request(
        self, publication_id: uuid.UUID, pr_number: int
    ) -> PublicationPullState:
        raise AssertionError("a failed Job has no stored pull request to read")

    async def verify_revision_commit(
        self,
        publication_id: uuid.UUID,
        commit_sha: str,
        *,
        revision_id: uuid.UUID,
        expected_parent: str,
    ) -> str:
        raise AssertionError("a failed Job has no revision commit to verify")

    async def recover_pull_request(
        self, publication_id: uuid.UUID, *, expected_head_sha: str, adopt_only: bool
    ) -> None:
        return None

    async def update_pull_request_metadata(self, publication_id: uuid.UUID) -> PublicationPullState:
        raise AssertionError("a patch publication never takes the metadata-only path")


@requires_cluster
async def test_real_git_failure_is_terminalized_once_without_spending_retry() -> None:
    engine = create_async_engine(_required("TEST_DATABASE_URL"))
    try:
        base_sha = await _base_sha()
        await _seed_publication(engine, base_sha)
        store = PostgresPublicationStore(
            engine,
            schema="curie",
            lease_owner="publication-cluster-proof",
            lease_seconds=5,
            result_max_attempts=2,
            reconcile_max_attempts=3,
        )
        work = await store.claim_next()
        assert work is not None
        assert work.publication_id == FAILURE_ID
        cluster = _cluster()
        credentials = _Credentials()
        replies = _Replies()
        transcript = _Transcript()
        async with httpx.AsyncClient(
            verify=_ssl_context(),
            timeout=10,
            event_hooks={"request": [_forward_fixture_request]},
        ) as client:
            reconciler = PublicationReconciler(
                store=store,
                credentials=credentials,
                cluster=cluster,
                # The Job fails before any success marker, so the API's code
                # host routes are asked at most for the absent branch.
                code_host=_NoBranchCodeHost(),
                replies=replies,
                # This Job fails before any success marker, so the API is never called.
                lineage=PublicationLineageClient(
                    api_base_url="http://curie-api.invalid",
                    worker_token="fixture-worker-token",
                    client=client,
                ),
                transcript=transcript,
                job_settings=_settings(),
            )
            await reconciler.reconcile(work)
            observation = await _wait_terminal(
                cluster,
                f"curie-publication-{FAILURE_ID.hex[:20]}",
            )
            assert observation.phase == "failed"
            assert observation.transport == _transport()
            assert observation.error is not None
            assert "error: No valid patches in input" in observation.error
            assert "container exited" in observation.error

            # The first pass released its lease while the Job was in flight.
            # Reclaim before processing the terminal observation, as the drain does.
            work = await store.claim_next()
            assert work is not None
            assert work.publication_id == FAILURE_ID
            await reconciler.reconcile(work)
            async with engine.connect() as connection:
                terminal = (
                    (
                        await connection.execute(
                            text(
                                "SELECT status, error, patch_bytes, reconcile_attempts, "
                                "terminal_at, resource_cleanup_completed_at, version, "
                                "result_reported_at "
                                "FROM curie.publications WHERE id = :id"
                            ),
                            {"id": FAILURE_ID},
                        )
                    )
                    .mappings()
                    .one()
                )
            assert terminal["status"] == "failed"
            assert terminal["patch_bytes"] is None
            assert terminal["reconcile_attempts"] == 0
            assert terminal["terminal_at"] is not None
            assert terminal["resource_cleanup_completed_at"] is not None
            assert terminal["result_reported_at"] is not None
            assert "error: No valid patches in input" in terminal["error"]
            assert terminal["error"].endswith(
                f"Nothing was pushed to {REPO_FULL_NAME}; "
                "ask again to request a new publication approval."
            )
            assert replies.texts == [
                f"Publication failed safely after approval: {terminal['error']}"
            ]
            assert transcript.texts == replies.texts

            before_replay = dict(terminal)
            await reconciler.reconcile(work)
            async with engine.connect() as connection:
                replay = (
                    (
                        await connection.execute(
                            text(
                                "SELECT status, error, patch_bytes, reconcile_attempts, "
                                "terminal_at, resource_cleanup_completed_at, version, "
                                "result_reported_at "
                                "FROM curie.publications WHERE id = :id"
                            ),
                            {"id": FAILURE_ID},
                        )
                    )
                    .mappings()
                    .one()
                )
            assert dict(replay) == before_replay
            assert len(replies.texts) == 1
            assert transcript.texts == replies.texts
    finally:
        await engine.dispose()


@requires_cluster
async def test_generated_script_only_pushes_the_marked_commit() -> None:
    cluster = _cluster()
    base_sha = await _base_sha()
    patch = (
        b"diff --git a/README.md b/README.md\n"
        b"--- a/README.md\n"
        b"+++ b/README.md\n"
        b"@@ -1 +1 @@\n"
        b"-base\n"
        b"+published by cluster proof\n"
    )
    payload = _payload(POSITIVE_ID, base_sha=base_sha, patch=patch)
    resources = build_publication_resources(
        payload,
        credential=AUTHORIZATION,
        settings=_settings(),
    )
    assert resources.job["spec"]["template"]["spec"]["containers"][0]["command"] == [
        "/bin/bash",
        "/publication/publish.sh",
    ]

    cluster.apply(resources)
    actual_config_map = cluster._core.read_namespaced_config_map(
        resources.names.config_map,
        _namespace(),
    )
    actual_job = cluster._batch.read_namespaced_job(
        resources.names.job,
        _namespace(),
    )
    assert actual_config_map.data["publish.sh"] == resources.config_map["data"]["publish.sh"]
    assert actual_job.spec.template.spec.containers[0].command == [
        "/bin/bash",
        "/publication/publish.sh",
    ]

    observation = await _wait_terminal(cluster, resources.names.job)
    assert observation.phase == "succeeded", observation.error
    assert observation.error is None
    assert observation.transport == payload.transport
    assert observation.commit_sha is not None
    assert observation.commit_sha != base_sha
    assert f"CURIE_COMMIT_SHA={observation.commit_sha}" in observation.logs
    # The Job is push-only: it emits no pull request marker and the code host
    # holds no pull request afterwards. The worker asks the API to open one.
    assert "CURIE_PR_" not in observation.logs

    branch_path = quote(payload.branch, safe="")
    ref_response = await _fixture_get(f"/repos/{REPO_FULL_NAME}/git/ref/heads/{branch_path}")
    assert ref_response.status_code == 200, ref_response.text
    assert ref_response.json()["object"]["sha"] == observation.commit_sha
    state_response = await _fixture_get("/__fixture/state")
    assert state_response.status_code == 200, state_response.text
    assert state_response.json()["pull"] is None


def _image_bound_contract_hash(
    payload: PublicationPayload, settings: PublicationJobSettings
) -> str:
    contract = {
        "publication_id": str(payload.publication_id),
        "revision_id": str(payload.revision_id),
        "revision_number": payload.revision_number,
        "repo_full_name": payload.repo_full_name,
        "clean_clone_url": payload.clean_clone_url,
        "origin": payload.transport.origin,
        "header_form": payload.transport.header_form,
        "ca_bundle_ref": None,
        "ca_bundle_config_map": None,
        "ca_bundle_key": None,
        "base_sha": payload.base_sha,
        "expected_prior_head": payload.expected_prior_head,
        "expected_remote_head": payload.expected_remote_head,
        "patch_sha256": hashlib.sha256(payload.patch).hexdigest(),
        "branch": payload.branch,
        "title": payload.title,
        "branch_prefix": payload.branch_prefix,
        "runner_image": settings.runner_image,
        "service_account_name": settings.service_account_name,
        "active_deadline_seconds": settings.active_deadline_seconds,
        "git_timeout_seconds": settings.git_timeout_seconds,
    }
    return hashlib.sha256(
        json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


async def _receive_barrier(branch: str, action: str) -> dict[str, bool]:
    async with httpx.AsyncClient(verify=_ssl_context(), timeout=10) as client:
        response = await client.post(
            f"{_required('CURIE_PUBLICATION_FIXTURE_API')}/__fixture/barrier",
            headers={"Authorization": AUTHORIZATION},
            json={"branch": branch, "action": action},
        )
    assert response.status_code == 200, response.text
    return dict(response.json())


async def _wait_receive_barrier(branch: str, stage: str) -> None:
    deadline = time.monotonic() + 30
    while (remaining := deadline - time.monotonic()) > 0:
        if (await _receive_barrier(branch, "state"))[stage]:
            return
        await asyncio.sleep(min(0.1, remaining))
    pytest.fail(f"publication Job did not reach its {stage} receive barrier")


class _RecordingPublicationCluster(KubernetesPublicationCluster):
    def __init__(self) -> None:
        super().__init__(_namespace(), kubeconfig=_required("CURIE_PUBLICATION_KUBECONFIG"))
        self.applications: list[str] = []
        self.quiescence_checks: list[tuple[str, bool]] = []
        self.cleanups: list[str] = []

    def apply(self, resources: Any) -> None:
        self.applications.append(resources.names.job)
        super().apply(resources)

    def rejected_job_quiescent(self, job_name: str, job_uid: str) -> bool:
        quiescent = super().rejected_job_quiescent(job_name, job_uid)
        self.quiescence_checks.append((job_uid, quiescent))
        return quiescent

    def cleanup_credentials(self, names: Any) -> None:
        self.cleanups.append("credentials")
        super().cleanup_credentials(names)

    def cleanup_terminal(self, names: Any) -> None:
        self.cleanups.append("terminal")
        super().cleanup_terminal(names)


@requires_cluster
async def test_job_from_another_runner_image_is_adopted_through_github(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_api.config import get_settings
    from curie_api.main import create_app

    worker_token = "publication-cluster-worker"
    monkeypatch.setenv("DATABASE_URL", _required("TEST_DATABASE_URL"))
    monkeypatch.setenv("CURIE_INTERNAL_WORKER_TOKEN", worker_token)
    monkeypatch.setenv("GITHUB_API_URL", _origin())
    monkeypatch.setenv("GITHUB_TOKEN", "fixture-token")
    monkeypatch.setenv("GITHUB_APP_ID", "")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", "")
    get_settings.cache_clear()
    engine = create_async_engine(_required("TEST_DATABASE_URL"))
    payload: PublicationPayload | None = None
    barriers_armed = False
    try:
        base_sha = await _base_sha()
        patch = (
            b"diff --git a/README.md b/README.md\n"
            b"--- a/README.md\n"
            b"+++ b/README.md\n"
            b"@@ -1 +1 @@\n"
            b"-base\n"
            b"+published across a worker upgrade\n"
        )
        await _seed_publication(engine, base_sha, publication_id=UPGRADE_ID, patch=patch)
        store = PostgresPublicationStore(
            engine, schema="curie", lease_owner="publication-upgrade-proof", lease_seconds=30
        )
        work = await store.claim_next()
        assert work is not None
        assert work.publication_id == UPGRADE_ID
        payload = _payload(UPGRADE_ID, base_sha=base_sha, patch=patch)
        settings_a = dataclasses.replace(
            _settings(), git_timeout_seconds=60, active_deadline_seconds=180
        )
        repository, separator, _tag = settings_a.runner_image.rpartition(":")
        assert separator and "@" not in repository
        settings_b = dataclasses.replace(settings_a, runner_image=f"{repository}:adopting-release")
        assert settings_a.runner_image != settings_b.runner_image
        resources = build_publication_resources(
            payload, credential=AUTHORIZATION, settings=settings_a
        )
        expected = build_publication_resources(
            payload, credential=AUTHORIZATION, settings=settings_b
        )
        old_hash = _image_bound_contract_hash(payload, settings_a)
        annotation = "curietech.ai/publication-contract-sha256"
        assert expected.job["metadata"]["annotations"][annotation] != old_hash
        for resource in (resources.config_map, resources.secret, resources.job):
            resource["metadata"]["annotations"][annotation] = old_hash

        await _receive_barrier(payload.branch, "arm")
        barriers_armed = True
        cluster = _RecordingPublicationCluster()
        cluster.apply(resources)
        await _wait_receive_barrier(payload.branch, "before")
        rejected = cluster.observe(resources.names.job)
        assert rejected.phase == "running"
        assert rejected.job_uid
        assert rejected.commit_sha is None
        live_job = cluster._batch.read_namespaced_job(resources.names.job, _namespace())
        assert live_job.metadata.uid == rejected.job_uid
        assert live_job.spec.template.spec.containers[0].image == settings_a.runner_image
        assert live_job.metadata.annotations[annotation] == old_hash
        with pytest.raises(PublicationResourceError, match="metadata contract"):
            cluster.validate_existing(expected)

        api_requests: list[tuple[str, str]] = []
        provider_requests: list[tuple[str, str]] = []

        async def record_api(request: httpx.Request) -> None:
            api_requests.append((request.method, request.url.path))

        async def record_provider(request: httpx.Request) -> None:
            provider_requests.append((request.method, request.url.path))
            await _forward_fixture_request(request)

        credentials = _Credentials()
        replies = _Replies()
        transcript = _Transcript()
        async with httpx.AsyncClient(
            verify=_ssl_context(), timeout=10, event_hooks={"request": [record_provider]}
        ) as provider_client:
            app = create_app()
            app.state.sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
            app.state.http_client = provider_client
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://publication-api.test",
                event_hooks={"request": [record_api]},
            ) as api_client:
                reconciler = PublicationReconciler(
                    store=store,
                    credentials=credentials,
                    cluster=cluster,
                    code_host=PublicationCodeHostClient(
                        api_base_url="http://publication-api.test",
                        worker_token=worker_token,
                        client=api_client,
                    ),
                    lineage=PublicationLineageClient(
                        api_base_url="http://publication-api.test",
                        worker_token=worker_token,
                        client=api_client,
                    ),
                    replies=replies,
                    transcript=transcript,
                    job_settings=settings_b,
                )
                branch_path = quote(payload.branch, safe="")
                branch_url = f"/repos/{REPO_FULL_NAME}/git/ref/heads/{branch_path}"
                absent = await _fixture_get(branch_url)
                assert absent.status_code == 404, absent.text

                async def assert_waiting() -> None:
                    async with engine.connect() as connection:
                        row = (
                            (
                                await connection.execute(
                                    text(
                                        "SELECT status, reconcile_attempts, terminal_at, "
                                        "lease_owner, resource_cleanup_completed_at "
                                        "FROM curie.publications WHERE id = :id"
                                    ),
                                    {"id": UPGRADE_ID},
                                )
                            )
                            .mappings()
                            .one()
                        )
                    assert dict(row) == {
                        "status": "launching",
                        "reconcile_attempts": 0,
                        "terminal_at": None,
                        "lease_owner": None,
                        "resource_cleanup_completed_at": None,
                    }
                    assert credentials.calls == []
                    assert api_requests == []
                    assert provider_requests == []
                    assert cluster.applications == [resources.names.job]
                    assert cluster.cleanups == []
                    assert replies.texts == []
                    assert transcript.texts == []
                    assert (
                        cluster._batch.read_namespaced_job(
                            resources.names.job, _namespace()
                        ).metadata.uid
                        == rejected.job_uid
                    )
                    assert cluster._core.read_namespaced_secret(
                        resources.names.secret, _namespace()
                    ).metadata.uid
                    state = await _fixture_get("/__fixture/state")
                    assert state.status_code == 200, state.text
                    assert state.json()["pull"] is None

                await reconciler.reconcile(work)
                await assert_waiting()
                assert cluster.quiescence_checks == [(rejected.job_uid, False)]

                await _receive_barrier(payload.branch, "before")
                await _wait_receive_barrier(payload.branch, "after")
                pushed = await _fixture_get(branch_url)
                assert pushed.status_code == 200, pushed.text
                head_sha = pushed.json()["object"]["sha"]
                assert head_sha != base_sha
                assert cluster.observe(resources.names.job).phase == "running"
                work = await store.claim_next()
                assert work is not None and work.publication_id == UPGRADE_ID
                await reconciler.reconcile(work)
                await assert_waiting()
                assert cluster.quiescence_checks == [(rejected.job_uid, False)] * 2

                await _receive_barrier(payload.branch, "after")
                terminal = await _wait_terminal(cluster, resources.names.job)
                assert terminal.phase == "succeeded", terminal.error
                assert terminal.job_uid == rejected.job_uid
                deadline = time.monotonic() + 10
                while not cluster.rejected_job_quiescent(resources.names.job, rejected.job_uid):
                    remaining = deadline - time.monotonic()
                    assert remaining > 0, "the completed Job's publisher Pods never quiesced"
                    await asyncio.sleep(min(0.1, remaining))
                work = await store.claim_next()
                assert work is not None and work.publication_id == UPGRADE_ID
                await reconciler.reconcile(work)

                async with engine.connect() as connection:
                    result = (
                        (
                            await connection.execute(
                                text(
                                    "SELECT p.status, p.reconcile_attempts, p.error, "
                                    "p.terminal_at, p.resource_cleanup_completed_at, "
                                    "p.result_reported_at, p.result_url AS pr_url, "
                                    "l.pr_url AS lineage_url, "
                                    "l.head_sha FROM curie.publications p "
                                    "JOIN curie.thread_publication_lineages l "
                                    "ON l.id = p.lineage_id "
                                    "WHERE p.id = :id"
                                ),
                                {"id": UPGRADE_ID},
                            )
                        )
                        .mappings()
                        .one()
                    )
                pr_url = f"{_origin()}/{REPO_FULL_NAME}/pull/1"
                assert result["status"] == "succeeded"
                assert result["reconcile_attempts"] == 0
                assert result["error"] is None
                assert result["terminal_at"] is not None
                assert result["resource_cleanup_completed_at"] is not None
                assert result["result_reported_at"] is not None
                assert result["pr_url"] == result["lineage_url"] == pr_url
                assert result["head_sha"] == head_sha
                assert credentials.calls == [UPGRADE_ID]
                assert cluster.applications == [resources.names.job]
                assert cluster.quiescence_checks[-1] == (rejected.job_uid, True)
                assert cluster.cleanups == ["credentials", "terminal"]
                assert replies.texts == [f"Published the approved changes: {pr_url}"]
                assert transcript.texts == replies.texts
                commit_read = ("GET", f"/repos/{REPO_FULL_NAME}/git/commits/{head_sha}")
                assert commit_read in provider_requests
                assert provider_requests.count(("POST", f"/repos/{REPO_FULL_NAME}/pulls")) == 1
                assert (
                    api_requests.count(
                        ("POST", f"/v1/internal/publications/{UPGRADE_ID}/revision-commit")
                    )
                    == 1
                )
                commit = await _fixture_get(f"/repos/{REPO_FULL_NAME}/git/commits/{head_sha}")
                assert commit.status_code == 200, commit.text
                assert commit.json()["sha"] == head_sha
                assert commit.json()["parents"] == [{"sha": base_sha}]
                assert f"Curie-Revision: {UPGRADE_ID}" in commit.json()["message"].splitlines()
                await reconciler.reconcile(work)
                assert credentials.calls == [UPGRADE_ID]
                assert provider_requests.count(("POST", f"/repos/{REPO_FULL_NAME}/pulls")) == 1
                assert replies.texts == [f"Published the approved changes: {pr_url}"]
                for read, name in (
                    (cluster._batch.read_namespaced_job, resources.names.job),
                    (cluster._core.read_namespaced_secret, resources.names.secret),
                    (cluster._core.read_namespaced_config_map, resources.names.config_map),
                ):
                    with pytest.raises(k8s_client.ApiException) as absent_resource:
                        read(name, _namespace())
                    assert absent_resource.value.status == 404
    finally:
        try:
            if barriers_armed and payload is not None:
                await _receive_barrier(payload.branch, "before")
                await _receive_barrier(payload.branch, "after")
        finally:
            await engine.dispose()
            get_settings.cache_clear()


CONTRACT_CA_PEM = "-----BEGIN CERTIFICATE-----\nMIIBcontractonly\n-----END CERTIFICATE-----\n"


def _contract_settings(directory: Path) -> PublicationJobSettings:
    bundle = directory / "worker-code-host-trust.crt"
    bundle.write_text(CONTRACT_CA_PEM, encoding="utf-8")
    return PublicationJobSettings(
        namespace="curie-publication-contract",
        runner_image="curie-runner:contract",
        image_pull_policy="Never",
        image_pull_secrets=(),
        priority_class_name="",
        service_account_name=SERVICE_ACCOUNT,
        owner_name=OWNER_NAME,
        git_user_name="Curie Publisher",
        git_user_email="publisher@example.com",
        cpu_request="25m",
        cpu_limit="500m",
        memory_request="64Mi",
        memory_limit="512Mi",
        ephemeral_request="128Mi",
        ephemeral_limit="1Gi",
        ca_bundle_path=str(bundle),
    )


def _contract_payload(transport: PublicationTransport) -> PublicationPayload:
    return PublicationPayload(
        publication_id=TRANSPORT_ID,
        revision_id=TRANSPORT_ID,
        revision_number=1,
        repo_full_name=REPO_FULL_NAME,
        clean_clone_url=f"{transport.origin}/{REPO_FULL_NAME}.git",
        base_sha="a" * 40,
        expected_prior_head="a" * 40,
        expected_remote_head=None,
        patch=b"diff --git a/README.md b/README.md\n",
        branch=f"curie/publication-{TRANSPORT_ID.hex}",
        title="Publish contract proof",
        transport=transport,
    )


CONTRACT_TRANSPORT = PublicationTransport(
    origin="https://git.example.test/base",
    header_form="private_token",
    ca_bundle_ref="/etc/curie/code-host-trust/ca.pem",
)


def test_job_transport_reads_back_what_the_builder_wrote(tmp_path: Path) -> None:
    resources = build_publication_resources(
        _contract_payload(CONTRACT_TRANSPORT),
        credential="fixture-private-token",
        settings=_contract_settings(tmp_path),
    )
    assert job_transport(resources.job) == CONTRACT_TRANSPORT

    pod_spec = resources.job["spec"]["template"]["spec"]
    assert {
        "name": "code-host-trust",
        "mountPath": "/etc/curie/code-host-trust",
        "readOnly": True,
    } in pod_spec["containers"][0]["volumeMounts"]
    assert {
        "name": "code-host-trust",
        "configMap": {
            "name": resources.names.config_map,
            "items": [{"key": "code-host-ca.pem", "path": "ca.pem"}],
        },
    } in pod_spec["volumes"]


def test_adoption_refuses_a_job_whose_transport_was_altered(tmp_path: Path) -> None:
    settings = _contract_settings(tmp_path)
    built = build_publication_resources(
        _contract_payload(CONTRACT_TRANSPORT),
        credential="fixture-private-token",
        settings=settings,
    )
    # The untouched Job adopts against the expected set rebuilt from its own env.
    rebuilt = build_publication_resources(
        _contract_payload(job_transport(built.job) or CONTRACT_TRANSPORT),
        credential="validation-placeholder",
        settings=settings,
    )
    validate_adopted_resource("Job", rebuilt.job, built.job)

    # Redirect the existing Job's push origin, as the reconciler would see it:
    # the expected set is rebuilt from the altered env and still refused.
    altered = copy.deepcopy(built.job)
    for item in altered["spec"]["template"]["spec"]["containers"][0]["env"]:
        if item["name"] == "CODE_HOST_ORIGIN":
            item["value"] = "https://attacker.example.test"
    read_back = job_transport(altered)
    assert read_back is not None
    assert read_back.origin == "https://attacker.example.test"
    expected = build_publication_resources(
        _contract_payload(read_back),
        credential="validation-placeholder",
        settings=settings,
    )
    with pytest.raises(PublicationResourceError, match=r"refusing to adopt Job .*mismatch"):
        validate_adopted_resource("Job", expected.job, altered)
    # Against the original contract, the altered Job is refused just the same.
    with pytest.raises(PublicationResourceError, match=r"refusing to adopt Job .*mismatch"):
        validate_adopted_resource("Job", rebuilt.job, altered)

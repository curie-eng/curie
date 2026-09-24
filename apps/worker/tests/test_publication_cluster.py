"""Real cluster proof for the hardened publication Job contract."""

from __future__ import annotations

import asyncio
import copy
import os
import ssl
import time
import uuid
from typing import Any
from urllib.parse import quote

import httpx
import pytest
from channel_protocol.reply import ReplyAck
from curie_worker.publication_clients import GitHubPublicationLookup, PublicationLineageClient
from curie_worker.publication_k8s import (
    KubernetesPublicationCluster,
    PublicationJobSettings,
    PublicationPayload,
    PublicationResourceError,
    build_publication_resources,
)
from curie_worker.publication_loop import (
    PublicationCredential,
    PublicationJobObservation,
    PublicationReconciler,
)
from curie_worker.publication_store import PostgresPublicationStore
from kubernetes import client as k8s_client
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

OPT_IN = os.environ.get("CURIE_PUBLICATION_CLUSTER_PROOF") == "1"
pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(
        not OPT_IN,
        reason="set CURIE_PUBLICATION_CLUSTER_PROOF=1 through the owned cluster wrapper",
    ),
]

REPO_FULL_NAME = "acme-corp/acme-bot"
OWNER_NAME = "publication-owner"
SERVICE_ACCOUNT = "publication-runner"
AUTHORIZATION = "Bearer fixture-token"
PULL_URL = f"https://github.com/{REPO_FULL_NAME}/pull/1"
ADOPT_ID = uuid.UUID("20000001-2222-4222-8222-000000000001")
MISMATCH_ID = uuid.UUID("20000002-2222-4222-8222-000000000002")
FAILURE_ID = uuid.UUID("20000003-2222-4222-8222-000000000003")
POSITIVE_ID = uuid.UUID("20000004-2222-4222-8222-000000000004")
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
        github_timeout_seconds=20,
        github_api_url=_required("CURIE_PUBLICATION_FIXTURE_CLUSTER_API"),
    )


def _payload(
    publication_id: uuid.UUID,
    *,
    base_sha: str,
    patch: bytes,
) -> PublicationPayload:
    return PublicationPayload(
        publication_id=publication_id,
        revision_id=publication_id,
        revision_number=1,
        repo_full_name=REPO_FULL_NAME,
        clean_clone_url=f"https://github.com/{REPO_FULL_NAME}.git",
        base_sha=base_sha,
        expected_prior_head=base_sha,
        expected_remote_head=None,
        patch=patch,
        branch=f"curie/publication-{publication_id.hex}",
        pr_number=None,
        pr_url=None,
        title="Publish cluster proof",
        body="Approved publication cluster proof.",
    )


def _ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context(
        cafile=_required("CURIE_PUBLICATION_FIXTURE_CA")
    )


async def _fixture_get(path: str) -> httpx.Response:
    async with httpx.AsyncClient(verify=_ssl_context(), timeout=10) as client:
        response = await client.get(
            f"{_required('CURIE_PUBLICATION_FIXTURE_API')}{path}",
            headers={"Authorization": AUTHORIZATION},
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
    assert env["PR_NUMBER"] == ""
    assert env["PR_URL"] == ""
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
    observed_env = {
        item["name"]: item.get("value")
        for item in pod_spec["containers"][0]["env"]
    }
    assert observed_env["EXPECTED_REMOTE_HEAD"] is None
    assert observed_env["PR_NUMBER"] is None
    assert observed_env["PR_URL"] is None

    cluster.apply(resources)
    assert cluster._core.read_namespaced_config_map(
        resources.names.config_map, _namespace()
    ).metadata.uid
    assert cluster._core.read_namespaced_secret(
        resources.names.secret, _namespace()
    ).metadata.uid
    assert cluster._batch.read_namespaced_job(
        resources.names.job, _namespace()
    ).metadata.uid


async def test_success_markers_cannot_authorize_a_mismatched_job() -> None:
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
        (
            f"printf '%s\\n' 'CURIE_PR_URL={PULL_URL}' "
            "'CURIE_PR_NUMBER=1' "
            f"'CURIE_COMMIT_SHA={'b' * 40}'"
        ),
    ]
    cluster._core.create_namespaced_config_map(_namespace(), body=config_map)
    cluster._core.create_namespaced_secret(_namespace(), body=secret)
    cluster._batch.create_namespaced_job(_namespace(), body=planted_job)

    observation = await _wait_terminal(cluster, resources.names.job)
    assert observation.phase == "succeeded"
    assert observation.pr_url == PULL_URL
    assert observation.pr_number == 1
    assert observation.commit_sha == "b" * 40

    with pytest.raises(
        PublicationResourceError,
        match=rf"refusing to adopt Job {resources.names.job!r}: spec mismatch",
    ):
        cluster.apply(resources)


class _Credentials:
    def __init__(self) -> None:
        self.calls: list[uuid.UUID] = []

    async def redeem(self, publication_id: uuid.UUID) -> PublicationCredential:
        self.calls.append(publication_id)
        return PublicationCredential(
            clean_clone_url=f"https://github.com/{REPO_FULL_NAME}.git",
            authorization_header=AUTHORIZATION,
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


async def _seed_failed_publication(engine: AsyncEngine, base_sha: str) -> None:
    branch = f"curie/publication-{FAILURE_ID.hex}"
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO curie.agents (id, name, repo_full_name) "
                "VALUES (:id, 'acme-bot', :repo)"
            ),
            {"id": AGENT_ID, "repo": REPO_FULL_NAME},
        )
        await connection.execute(
            text(
                "INSERT INTO curie.agent_versions "
                "(id, agent_id, version_label, created_by) "
                "VALUES (:id, :agent_id, 'cluster-proof', 'fixture')"
            ),
            {"id": VERSION_ID, "agent_id": AGENT_ID},
        )
        await connection.execute(
            text(
                "INSERT INTO curie.deployments "
                "(id, agent_id, version_id, environment, workspace_enabled, status) "
                "VALUES (:id, :agent_id, :version_id, 'dev', true, 'active')"
            ),
            {
                "id": DEPLOYMENT_ID,
                "agent_id": AGENT_ID,
                "version_id": VERSION_ID,
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
                "'1700000000.000200', 'publication-cluster-proof', 'approved', "
                "'U0APPROVE1', 'Approved fixture publication.', 'publication')"
            ),
            {"id": APPROVAL_ID, "agent_id": AGENT_ID},
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
                "id": LINEAGE_ID,
                "agent_id": AGENT_ID,
                "deployment_id": DEPLOYMENT_ID,
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
                "id": FAILURE_ID,
                "approval_id": APPROVAL_ID,
                "deployment_id": DEPLOYMENT_ID,
                "lineage_id": LINEAGE_ID,
                "base_sha": base_sha,
                "repo": REPO_FULL_NAME,
                "patch": b"this is not a unified diff\n",
                "changed_paths": '["README.md"]',
            },
        )


async def test_real_git_failure_is_terminalized_once_without_spending_retry() -> None:
    engine = create_async_engine(_required("TEST_DATABASE_URL"))
    try:
        base_sha = await _base_sha()
        await _seed_failed_publication(engine, base_sha)
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
        async with httpx.AsyncClient(verify=_ssl_context(), timeout=10) as client:
            reconciler = PublicationReconciler(
                store=store,
                credentials=credentials,
                cluster=cluster,
                github=GitHubPublicationLookup(
                    client,
                    api_base_url=_required("CURIE_PUBLICATION_FIXTURE_API"),
                ),
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
                    await connection.execute(
                        text(
                            "SELECT status, error, patch_bytes, reconcile_attempts, "
                            "terminal_at, resource_cleanup_completed_at, version, "
                            "result_reported_at "
                            "FROM curie.publications WHERE id = :id"
                        ),
                        {"id": FAILURE_ID},
                    )
                ).mappings().one()
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
                    await connection.execute(
                        text(
                            "SELECT status, error, patch_bytes, reconcile_attempts, "
                            "terminal_at, resource_cleanup_completed_at, version, "
                            "result_reported_at "
                            "FROM curie.publications WHERE id = :id"
                        ),
                        {"id": FAILURE_ID},
                    )
                ).mappings().one()
            assert dict(replay) == before_replay
            assert len(replies.texts) == 1
            assert transcript.texts == replies.texts
    finally:
        await engine.dispose()


async def test_generated_script_pushes_and_creates_pull_request() -> None:
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
    assert resources.job["spec"]["template"]["spec"]["containers"][0][
        "command"
    ] == ["/bin/bash", "/publication/publish.sh"]

    cluster.apply(resources)
    actual_config_map = cluster._core.read_namespaced_config_map(
        resources.names.config_map,
        _namespace(),
    )
    actual_job = cluster._batch.read_namespaced_job(
        resources.names.job,
        _namespace(),
    )
    assert actual_config_map.data["publish.sh"] == resources.config_map["data"][
        "publish.sh"
    ]
    assert actual_job.spec.template.spec.containers[0].command == [
        "/bin/bash",
        "/publication/publish.sh",
    ]

    observation = await _wait_terminal(cluster, resources.names.job)
    assert observation.phase == "succeeded", observation.error
    assert observation.error is None
    assert observation.pr_url == PULL_URL
    assert observation.pr_number == 1
    assert observation.commit_sha is not None
    assert observation.commit_sha != base_sha
    assert f"CURIE_PR_URL={PULL_URL}" in observation.logs
    assert "CURIE_PR_NUMBER=1" in observation.logs
    assert f"CURIE_COMMIT_SHA={observation.commit_sha}" in observation.logs

    branch_path = quote(payload.branch, safe="")
    ref_response = await _fixture_get(
        f"/repos/{REPO_FULL_NAME}/git/ref/heads/{branch_path}"
    )
    assert ref_response.status_code == 200, ref_response.text
    assert ref_response.json()["object"]["sha"] == observation.commit_sha
    pulls_response = await _fixture_get(
        f"/repos/{REPO_FULL_NAME}/pulls?state=all&head="
        f"{quote('acme-corp:' + payload.branch, safe='')}"
    )
    assert pulls_response.status_code == 200, pulls_response.text
    pulls = pulls_response.json()
    assert len(pulls) == 1
    assert pulls[0]["html_url"] == PULL_URL
    assert pulls[0]["head"]["sha"] == observation.commit_sha
    assert pulls[0]["head"]["ref"] == payload.branch
    assert pulls[0]["base"]["sha"] == base_sha

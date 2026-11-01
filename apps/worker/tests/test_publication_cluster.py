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
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import jwt
import pytest
from channel_protocol.reply import ReplyAck
from curie_worker.publication_clients import (
    PublicationCodeHostClient,
    PublicationCredentialClient,
    PublicationLineageClient,
)
from curie_worker.publication_k8s import (
    KubernetesPublicationCluster,
    PublicationJobSettings,
    PublicationPayload,
    PublicationResourceError,
    PublicationTransport,
    build_publication_resources,
    job_transport,
    publication_resource_names,
    validate_adopted_resource,
)
from curie_worker.publication_loop import (
    PublicationBranchHead,
    PublicationCredential,
    PublicationJobObservation,
    PublicationPullState,
    PublicationReconciler,
    PublicationRevisionCommit,
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
APP_ID = "51"
INSTALLATION_ID = 41
APP_TOKEN = "fixture-publication-app-token"
APP_AUTHORIZATION = f"Bearer {APP_TOKEN}"
APP_REST_AUTHORIZATION = (
    "Basic " + base64.b64encode(f"x-access-token:{APP_TOKEN}".encode()).decode()
)
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
        base_branch="main",
        expected_prior_head=base_sha,
        expected_remote_head=None,
        patch=patch,
        branch=f"curie/publication-{publication_id.hex}",
        title="Publish cluster proof",
        transport=transport,
    )


def _ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context(cafile=_required("CURIE_PUBLICATION_FIXTURE_CA"))


def _forward_fixture_url(request: httpx.Request) -> None:
    """Reach the fixture port forward while retaining its configured forge identity."""
    if str(request.url).startswith(_required("CURIE_PUBLICATION_FIXTURE_CLUSTER_API") + "/"):
        forwarded = httpx.URL(_required("CURIE_PUBLICATION_FIXTURE_API"))
        request.url = request.url.copy_with(
            scheme=forwarded.scheme, host=forwarded.host, port=forwarded.port
        )


async def _forward_fixture_request(request: httpx.Request) -> None:
    _forward_fixture_url(request)


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
    # https://docs.github.com/en/rest/git/refs#get-a-reference
    assert branch.json() == {
        "ref": "refs/heads/main",
        "object": {"type": "commit", "sha": base_sha},
    }

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
async def test_fixture_app_routes_require_signed_jwt_and_minted_installation_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = await _configure_github_app(monkeypatch)
    now = int(time.time())
    claims = {"iss": APP_ID, "iat": now - 60, "exp": now + 480}
    signed = jwt.encode(claims, private_key, algorithm="RS256")
    jwt_authorization = f"Bearer {signed}"
    repo_path = f"/repos/{REPO_FULL_NAME}"
    token_path = f"/app/installations/{INSTALLATION_ID}/access_tokens"
    # https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-json-web-token-jwt-for-a-github-app
    invalid_jwts = [
        jwt.encode(
            {key: value for key, value in claims.items() if key != required},
            private_key,
            algorithm="RS256",
        )
        for required in claims
    ] + [
        jwt.encode({**claims, "iss": "52"}, private_key, algorithm="RS256"),
        jwt.encode(
            {**claims, "iat": now - 800, "exp": now - 200}, private_key, algorithm="RS256"
        ),
        jwt.encode({**claims, "iat": now + 120}, private_key, algorithm="RS256"),
        jwt.encode({**claims, "exp": now + 1200}, private_key, algorithm="RS256"),
        jwt.encode(
            claims,
            rsa.generate_private_key(public_exponent=65537, key_size=2048),
            algorithm="RS256",
        ),
        jwt.encode(claims, "fixture-wrong-app-secret-00000000", algorithm="HS256"),
        jwt.encode(claims, "", algorithm="none"),
    ]
    origin = _required("CURIE_PUBLICATION_FIXTURE_API")
    async with httpx.AsyncClient(verify=_ssl_context(), timeout=10) as client:
        for method, path in (("GET", f"{repo_path}/installation"), ("POST", token_path)):
            for authorization in (
                None,
                AUTHORIZATION,
                REST_AUTHORIZATION,
                APP_AUTHORIZATION,
                "Bearer malformed",
                *(f"Bearer {token}" for token in invalid_jwts),
            ):
                response = await client.request(
                    method,
                    f"{origin}{path}",
                    json={"repositories": ["acme-bot"]},
                    headers={"Authorization": authorization} if authorization is not None else {},
                )
                assert response.status_code == 401, response.text
        for authorization in (APP_AUTHORIZATION, APP_REST_AUTHORIZATION):
            unissued = await client.get(
                f"{origin}{repo_path}", headers={"Authorization": authorization}
            )
            assert unissued.status_code == 401, unissued.text
        installation = await client.get(
            f"{origin}{repo_path}/installation", headers={"Authorization": jwt_authorization}
        )
        assert installation.status_code == 200, installation.text
        assert installation.json() == {"id": INSTALLATION_ID}
        for repositories in ([], ["other-repo"]):
            refused = await client.post(
                f"{origin}{token_path}",
                json={"repositories": repositories},
                headers={"Authorization": jwt_authorization},
            )
            assert refused.status_code == 422, refused.text
        minted = await client.post(
            f"{origin}{token_path}",
            json={"repositories": ["acme-bot"]},
            headers={"Authorization": jwt_authorization},
        )
        assert minted.status_code == 201, minted.text
        assert minted.json()["token"] == APP_TOKEN
        for path, authorization, expected in (
            (repo_path, APP_AUTHORIZATION, 200),
            (repo_path, APP_REST_AUTHORIZATION, 200),
            (f"{repo_path}/git/ref/heads/main", APP_REST_AUTHORIZATION, 200),
            (f"{repo_path}/git/ref/heads/main", APP_AUTHORIZATION, 401),
            (repo_path, jwt_authorization, 401),
            ("/__fixture/state", APP_AUTHORIZATION, 401),
            ("/__fixture/state", APP_REST_AUTHORIZATION, 401),
            ("/__fixture/state", jwt_authorization, 401),
        ):
            response = await client.get(
                f"{origin}{path}", headers={"Authorization": authorization}
            )
            assert response.status_code == expected, response.text
        for authorization in (
            None,
            REST_AUTHORIZATION,
            APP_AUTHORIZATION,
            APP_REST_AUTHORIZATION,
            jwt_authorization,
        ):
            response = await client.post(
                f"{origin}/__fixture/app",
                json={"public_key": "invalid"},
                headers={"Authorization": authorization} if authorization is not None else {},
            )
            assert response.status_code == 401, response.text
        invalid_key = await client.post(
            f"{origin}/__fixture/app",
            json={"public_key": "invalid"},
            headers={"Authorization": AUTHORIZATION},
        )
        assert invalid_key.status_code == 400, invalid_key.text


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
) -> uuid.UUID:
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
    return lineage_id


async def _seed_publication_authority(engine: AsyncEngine, publication_id: uuid.UUID) -> None:
    agent_id = uuid.uuid5(publication_id, "agent")
    binding_id = uuid.uuid5(publication_id, "binding")
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO curie.thread_workspaces "
                "(id, agent_id, selected_by_deployment_id, conversation_id, repo_full_name, "
                "selected_by) VALUES (:id, :agent, :deployment, "
                "'slack:C0EXAMPLE1:1700000000.000100', :repo, 'fixture')"
            ),
            {
                "id": uuid.uuid4(),
                "agent": agent_id,
                "deployment": uuid.uuid5(publication_id, "deployment"),
                "repo": REPO_FULL_NAME,
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.agent_channels (id, agent_id, kind, address, adapter, "
                "generation) VALUES (:id, :agent, 'slack', 'C0EXAMPLE1', 'default', 1)"
            ),
            {"id": binding_id, "agent": agent_id},
        )
        await connection.execute(
            text(
                "UPDATE curie.thread_publication_lineages SET binding_id = :binding, "
                "binding_generation = 1, reply_conversation_id = '1700000000.000100' "
                "WHERE id = :id"
            ),
            {"id": uuid.uuid5(publication_id, "lineage"), "binding": binding_id},
        )


class _NoBranchCodeHost:
    """The API's code host routes for a publication whose branch was never pushed."""

    async def read_branch_head(self, publication_id: uuid.UUID) -> PublicationBranchHead:
        return PublicationBranchHead(head_sha=None, base_branch="main")

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
    ) -> PublicationRevisionCommit:
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


async def _fixture_post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(verify=_ssl_context(), timeout=20) as client:
        response = await client.post(
            f"{_required('CURIE_PUBLICATION_FIXTURE_API')}{path}",
            json=payload,
            headers={"Authorization": AUTHORIZATION},
        )
    assert response.status_code == 200, response.text
    return dict(response.json())


async def _configure_github_app(monkeypatch: pytest.MonkeyPatch) -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from curie_api.config import get_settings
    from curie_api.github_app import _RESOLVERS

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    configured = await _fixture_post(
        "/__fixture/app",
        {
            "public_key": (
                key.public_key()
                .public_bytes(
                    serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
                )
                .decode()
            )
        },
    )
    assert configured == {"app_id": APP_ID, "installation_id": INSTALLATION_ID}
    private_key = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    for name, value in {
        "GITHUB_APP_ID": APP_ID,
        "GITHUB_APP_PRIVATE_KEY": private_key,
        "GITHUB_TOKEN": "",
        "GITHUB_API_URL": _origin(),
        "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "CURIE_CODE_HOST_CA_BUNDLE": "",
        "CURIE_FORGES": "{}",
        "SSL_CERT_FILE": _required("CURIE_PUBLICATION_FIXTURE_CA"),
    }.items():
        monkeypatch.setenv(name, value)
    real_client = httpx.Client

    def forwarded_client(*args: Any, **kwargs: Any) -> httpx.Client:
        hooks = dict(kwargs.pop("event_hooks", {}))
        hooks["request"] = [*hooks.get("request", []), _forward_fixture_url]
        return real_client(*args, **kwargs, event_hooks=hooks)

    # The resolver owns its sync client; both clients use the real HTTPS fixture.
    monkeypatch.setattr("curie_api.github_app.httpx.Client", forwarded_client)
    _RESOLVERS.clear()
    get_settings.cache_clear()
    return private_key


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


async def _remove_scenario_resources(
    cluster: KubernetesPublicationCluster, publication_id: uuid.UUID
) -> None:
    names = publication_resource_names(publication_id)
    cluster.cleanup_credentials(names)
    cluster.cleanup_terminal(names)
    deadline = time.monotonic() + 30
    while True:
        pods = cluster._core.list_namespaced_pod(
            _namespace(), label_selector=f"job-name={names.job}"
        ).items
        remaining = []
        for read, name in (
            (cluster._batch.read_namespaced_job, names.job),
            (cluster._core.read_namespaced_config_map, names.config_map),
            (cluster._core.read_namespaced_secret, names.secret),
        ):
            try:
                read(name, _namespace())
            except k8s_client.ApiException as exc:
                if exc.status != 404:
                    raise
            else:
                remaining.append(name)
        if not pods and not remaining:
            return
        assert time.monotonic() < deadline, "scenario publication resources were not deleted"
        await asyncio.sleep(0.25)


@asynccontextmanager
async def _moved_cluster_scenario(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[dict[str, Any]]:
    from curie_api.config import get_settings
    from curie_api.forges.registry import forges_registry
    from curie_api.github_app import _RESOLVERS
    from curie_api.routers import publication_code_host, publications
    from fastapi import FastAPI

    publication_id = uuid.uuid4()
    branch = f"curie/publication-{publication_id.hex}"
    engine = create_async_engine(_required("TEST_DATABASE_URL"))
    cluster = _cluster()
    scenario = await _fixture_post("/__fixture/scenario", {"branch": branch})
    scenario_id = scenario["scenario_id"]
    try:
        for name, value in {
            "DATABASE_URL": _required("TEST_DATABASE_URL"),
            "INTERNAL_WORKER_TOKEN": "publication-cluster-worker",
            "GITHUB_CLONE_BASE": _origin(),
        }.items():
            monkeypatch.setenv(name, value)
        await _configure_github_app(monkeypatch)
        lineage_id = await _seed_publication(
            engine,
            scenario["original_base_sha"],
            publication_id=publication_id,
            patch=scenario["patch"].encode(),
        )
        await _seed_publication_authority(engine, publication_id)

        async with httpx.AsyncClient(
            verify=_ssl_context(),
            timeout=10,
            event_hooks={"request": [_forward_fixture_request]},
        ) as external:
            app = FastAPI()
            app.state.sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
            app.state.http_client = external
            app.state.forges = forges_registry(get_settings())
            app.include_router(publications.internal_router)
            app.include_router(publication_code_host.router)
            requests: list[tuple[str, str]] = []

            async def record(request: httpx.Request) -> None:
                requests.append((request.method, request.url.path))

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://api.example.test",
                event_hooks={"request": [record]},
            ) as internal:
                store = PostgresPublicationStore(
                    engine,
                    schema="curie",
                    lease_owner=f"publication-proof-{publication_id.hex}",
                    lease_seconds=30,
                )
                replies, transcript = _Replies(), _Transcript()
                reconciler = PublicationReconciler(
                    store=store,
                    credentials=PublicationCredentialClient(
                        api_base_url="http://api.example.test",
                        worker_token="publication-cluster-worker",
                        client=internal,
                    ),
                    cluster=cluster,
                    code_host=PublicationCodeHostClient(
                        api_base_url="http://api.example.test",
                        worker_token="publication-cluster-worker",
                        client=internal,
                    ),
                    lineage=PublicationLineageClient(
                        api_base_url="http://api.example.test",
                        worker_token="publication-cluster-worker",
                        client=internal,
                    ),
                    replies=replies,
                    transcript=transcript,
                    job_settings=_settings(),
                )
                yield {
                    **scenario,
                    "publication_id": publication_id,
                    "lineage_id": lineage_id,
                    "cluster": cluster,
                    "store": store,
                    "reconciler": reconciler,
                    "engine": engine,
                    "requests": requests,
                    "replies": replies,
                }
    finally:
        try:
            await _remove_scenario_resources(cluster, publication_id)
        finally:
            try:
                try:
                    async with engine.begin() as connection:
                        await connection.execute(
                            text(
                                "DELETE FROM curie.credential_redemption_audit_entries WHERE "
                                "publication_id = :id"
                            ),
                            {"id": publication_id},
                        )
                        await connection.execute(
                            text("DELETE FROM curie.agents WHERE id = :id"),
                            {"id": uuid.uuid5(publication_id, "agent")},
                        )
                finally:
                    restored = await _fixture_post(
                        "/__fixture/restore", {"scenario_id": scenario_id}
                    )
                    assert restored == {
                        "base_sha": scenario["original_base_sha"],
                        "head_sha": None,
                        "restored": True,
                    }
            finally:
                await engine.dispose()
                _RESOLVERS.clear()
                get_settings.cache_clear()


async def _scenario_sql(case: dict[str, Any]) -> dict[str, Any]:
    async with case["engine"].connect() as connection:
        result = await connection.execute(
            text(
                "SELECT p.base_sha AS approved_base, p.expected_prior_head, p.status, p.error, "
                "p.result_url, p.patch_bytes, p.reconcile_attempts, l.base_sha AS lineage_base, "
                "l.head_sha, l.pr_number, l.pr_url, l.repository_project_id, "
                "l.version AS lineage_version "
                "FROM curie.publications p JOIN curie.thread_publication_lineages l ON l.id = "
                "p.lineage_id "
                "WHERE p.id = :id"
            ),
            {"id": case["publication_id"]},
        )
        return dict(result.mappings().one())


@requires_cluster
async def test_moved_base_real_job_opens_pull_request_on_current_tip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _moved_cluster_scenario(monkeypatch) as case:
        original = case["original_base_sha"]
        moved = await _fixture_post("/__fixture/move-base", {"scenario_id": case["scenario_id"]})
        parent = moved["tip_sha"]
        assert moved["original_base_sha"] == original
        assert parent != original
        store, reconciler, cluster = case["store"], case["reconciler"], case["cluster"]
        work = await store.claim_next()
        assert work is not None and work.publication_id == case["publication_id"]
        await reconciler.reconcile(work)
        names = publication_resource_names(work.publication_id)
        job = cluster._batch.read_namespaced_job(names.job, _namespace())
        uid = job.metadata.uid
        assert uid
        assert job.spec.template.spec.containers[0].command == [
            "/bin/bash",
            "/publication/publish.sh",
        ]
        observation = await _wait_terminal(cluster, names.job)
        assert observation.phase == "succeeded", observation.error
        assert observation.commit_sha is not None and observation.transport is not None
        pushed = observation.commit_sha
        assert "CURIE_PR_" not in observation.logs

        secret = cluster._core.read_namespaced_secret(names.secret, _namespace())
        credential = base64.b64decode(secret.data["credential"], validate=True).decode()
        expected = build_publication_resources(
            _payload(
                work.publication_id,
                base_sha=original,
                patch=case["patch"].encode(),
                transport=observation.transport,
            ),
            credential=credential,
            settings=_settings(),
        )
        assert (
            cluster._core.read_namespaced_config_map(names.config_map, _namespace()).data[
                "publish.sh"
            ]
            == expected.config_map["data"]["publish.sh"]
        )
        cluster.apply(expected)
        cluster.validate_existing(expected)
        assert cluster._batch.read_namespaced_job(names.job, _namespace()).metadata.uid == uid

        later = await _fixture_post("/__fixture/move-base", {"scenario_id": case["scenario_id"]})
        assert later["tip_sha"] not in {original, parent, pushed}
        work = await store.claim_next()
        assert work is not None and work.publication_id == case["publication_id"]
        await reconciler.reconcile(work)

        stored = await _scenario_sql(case)
        assert stored["status"] == "succeeded", stored["error"]
        assert stored["approved_base"] == stored["expected_prior_head"] == original
        assert stored["lineage_base"] == parent
        assert stored["head_sha"] == pushed
        assert stored["pr_number"] == 1
        assert stored["repository_project_id"] == "9001"
        assert stored["result_url"] == stored["pr_url"]
        assert stored["patch_bytes"] is None
        inspection = await _fixture_post("/__fixture/inspect", {"scenario_id": case["scenario_id"]})
        assert inspection["commit"]["sha"] == pushed
        assert inspection["commit"]["parents"] == [{"sha": parent}]
        assert inspection["diff"] == case["patch"]
        assert inspection["files"]["README.md"] == "published by moved base proof"
        assert inspection["files"][".github/workflows/fixture.yaml"].startswith("name: Fixture 1\n")
        assert inspection["pull"]["head"]["sha"] == pushed
        assert inspection["pull"]["base"]["ref"] == "main"
        assert inspection["pull"]["base"]["sha"] == later["tip_sha"]
        assert inspection["pushes"] == inspection["pull_creations"] == 1

        # Replay the pre-response work after SQL has committed, as after a lost response.
        await reconciler.reconcile(work)
        assert await _scenario_sql(case) == stored
        assert (
            await _fixture_post("/__fixture/inspect", {"scenario_id": case["scenario_id"]})
            == inspection
        )
        assert (
            case["requests"].count(
                ("POST", f"/v1/internal/publications/{work.publication_id}/credential")
            )
            == 1
        )
        assert any(
            method == "PATCH" and path.endswith("/lineage") for method, path in case["requests"]
        )


@requires_cluster
async def test_moved_base_conflict_real_job_records_named_failure_without_branch_or_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with _moved_cluster_scenario(monkeypatch) as case:
        moved = await _fixture_post(
            "/__fixture/move-base",
            {
                "scenario_id": case["scenario_id"],
                "conflict": True,
            },
        )
        store, reconciler, cluster = case["store"], case["reconciler"], case["cluster"]
        work = await store.claim_next()
        assert work is not None and work.publication_id == case["publication_id"]
        await reconciler.reconcile(work)
        observation = await _wait_terminal(
            cluster, publication_resource_names(work.publication_id).job
        )
        assert observation.phase == "failed"
        assert observation.commit_sha is None
        assert "CURIE_COMMIT_SHA=" not in observation.logs
        work = await store.claim_next()
        assert work is not None and work.publication_id == case["publication_id"]
        await reconciler.reconcile(work)
        stored = await _scenario_sql(case)
        assert stored["status"] == "failed"
        assert "publication_base_moved_conflict" in stored["error"]
        assert case["original_base_sha"] in stored["error"]
        assert moved["tip_sha"] in stored["error"]
        assert stored["lineage_base"] == stored["approved_base"] == case["original_base_sha"]
        assert stored["head_sha"] is None and stored["pr_number"] is None
        assert stored["reconcile_attempts"] == 0
        inspection = await _fixture_post("/__fixture/inspect", {"scenario_id": case["scenario_id"]})
        assert inspection["head_sha"] is None and inspection["pull"] is None
        assert inspection["pushes"] == inspection["pull_creations"] == 0


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
    from curie_api.github_app import _RESOLVERS
    from curie_api.main import create_app

    worker_token = "publication-cluster-worker"
    monkeypatch.setenv("DATABASE_URL", _required("TEST_DATABASE_URL"))
    monkeypatch.setenv("CURIE_INTERNAL_WORKER_TOKEN", worker_token)
    await _configure_github_app(monkeypatch)
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
        await _seed_publication_authority(engine, UPGRADE_ID)
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
                provider_branch_path = f"/repos/{REPO_FULL_NAME}/git/ref/heads/{payload.branch}"
                absent = await _fixture_get(branch_url)
                assert absent.status_code == 404, absent.text

                async def assert_waiting(base_reads: int) -> None:
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
                    assert api_requests == [
                        ("GET", f"/v1/internal/publications/{UPGRADE_ID}/branch-head")
                    ] * base_reads
                    assert provider_requests == [
                        ("GET", f"/repos/{REPO_FULL_NAME}"),
                        ("GET", provider_branch_path),
                        (
                            "GET",
                            f"/repos/{REPO_FULL_NAME}/git/matching-refs/heads/{payload.branch}",
                        ),
                    ] + [
                        ("GET", f"/repos/{REPO_FULL_NAME}"),
                        ("GET", provider_branch_path),
                    ] * (base_reads - 1)
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
                await assert_waiting(1)
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
                await assert_waiting(2)
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
                                    "l.head_sha, l.repository_project_id "
                                    "FROM curie.publications p "
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
                assert result["repository_project_id"] == "9001"
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
            _RESOLVERS.clear()
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
        base_branch="main",
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

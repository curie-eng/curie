"""The production worker client reaches authenticated recovery HTTP routes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_worker.workspace_recovery import WorkspaceRecoveryClient
from fastapi.testclient import TestClient
from test_workspace_recovery import (
    _authority,
    _completion,
    _reader_body,
    _reserve_body,
    _seed,
    _snapshot,
)
from test_workspace_recovery import recovery as recovery


def _drive(
    client: TestClient,
    operation: Callable[[WorkspaceRecoveryClient], Awaitable[Any]],
    *,
    worker_token: str | None = None,
) -> Any:
    async def invoke() -> Any:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=client.app),
            base_url="http://testserver",
        ) as http:
            worker = WorkspaceRecoveryClient(
                api_base_url="http://testserver",
                worker_token=(
                    get_settings().internal_worker_token
                    if worker_token is None
                    else worker_token
                ),
                client=http,
            )
            return await operation(worker)

    assert client.portal is not None
    return client.portal.call(invoke)


def test_worker_client_preserves_complete_checkpoint_during_failed_capture_collection(
    recovery: None, client: TestClient,
) -> None:
    source = _seed()

    async def exercise(worker: WorkspaceRecoveryClient) -> None:
        first = await worker.reserve(_reserve_body(source))
        complete = await worker.complete(_completion(source, first))
        assert complete["checkpoint_id"] == first["checkpoint_id"]
        assert complete["status"] == "complete"

        reader = await worker.acquire_reader(_reader_body(source, first))
        assert reader["checkpoint_id"] == first["checkpoint_id"]
        assert {obj["kind"]: obj for obj in reader["objects"]} == {
            obj["kind"]: obj for obj in first["objects"]
        }
        await worker.release_reader({
            "lease_id": reader["lease_id"], "holder_id": reader["holder_id"],
        })

        staging = await worker.reserve(_reserve_body(source, reuse=True))
        assert {obj["kind"] for obj in staging["objects"]} == {"patch", "manifest"}
        await worker.fail({
            **_authority(source),
            "checkpoint_id": staging["checkpoint_id"],
            "reason": "upload_failed",
        })
        collector = {"collector_id": "client-test-collector", "limit": 100}
        collection = await worker.claim_collection(collector)
        assert {claim["key"] for claim in collection["claims"]} == {
            obj["key"] for obj in staging["objects"]
        }
        for claim in collection["claims"]:
            await worker.finalize_collection({
                "collector_id": collector["collector_id"],
                "object_id": claim["object_id"],
                "claim_id": claim["claim_id"],
                "version": claim["version"],
            })
        assert (await worker.claim_collection(collector))["claims"] == []

        retained = await worker.acquire_reader(_reader_body(source, first, "after-collection"))
        assert retained["checkpoint_id"] == first["checkpoint_id"]
        assert {obj["kind"]: obj for obj in retained["objects"]} == {
            obj["kind"]: obj for obj in first["objects"]
        }
        await worker.release_reader({
            "lease_id": retained["lease_id"], "holder_id": retained["holder_id"],
        })

    _drive(client, exercise)


@pytest.mark.parametrize("refusal", ("credential", "runtime_epoch"))
def test_worker_client_surfaces_authority_refusal_without_writes(
    recovery: None, client: TestClient, refusal: str,
) -> None:
    source = _seed()
    body = _reserve_body(source)
    token = get_settings().internal_worker_token
    if refusal == "credential":
        token = "wrong-worker-token"
    else:
        body["runtime_epoch"] += 1
    before = _snapshot()

    async def reserve(worker: WorkspaceRecoveryClient) -> dict[str, Any]:
        return await worker.reserve(body)

    with pytest.raises(httpx.HTTPStatusError) as error:
        _drive(client, reserve, worker_token=token)
    response = error.value.response
    assert response.status_code == (401 if refusal == "credential" else 409)
    if refusal == "runtime_epoch":
        assert response.json()["detail"]["code"].startswith("workspace_recovery.")
    assert _snapshot() == before


def test_worker_client_surfaces_disabled_recovery_without_writes(
    recovery: None, client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _seed()
    body = _reserve_body(source)
    monkeypatch.setenv("CURIE_WORKSPACE_RECOVERY_ENABLED", "false")
    get_settings.cache_clear()
    before = _snapshot()

    async def reserve(worker: WorkspaceRecoveryClient) -> dict[str, Any]:
        return await worker.reserve(body)

    with pytest.raises(httpx.HTTPStatusError) as error:
        _drive(client, reserve)
    assert error.value.response.status_code == 503
    assert error.value.response.json()["detail"]["code"].startswith("workspace_recovery.")
    assert _snapshot() == before

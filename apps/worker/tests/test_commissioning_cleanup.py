"""Retirement of a campaign Pod whose runner container never started.

@spec PROTECTED-COMMISSIONING-8. The shared mechanics prove termination of the
original runner container from its recorded identity; a Pod deleted before
that container existed has none. The commissioning cleanup records instead that
no container ever started, observed only on the exact owned Pod after deletion
once no container can start any more, and only then removes the protected
finalizer with the finalizer-only update. Absence, a pending kubelet or any
container identity never pass.
"""

from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from typing import Any

import httpx
import pytest
from aiohttp import web
from curie_worker import commissioning_cleanup as cleanup
from curie_worker.commissioning_registry import object_names
from curie_worker.protected_launch_kubernetes import FINALIZER
from curie_worker.protected_launch_registry import RegistryError

from .protected_launch_endpoint_helpers import context
from .test_commissioning_launch import (
    OWNER,
    ROUTE,
    Cluster,
    mediator,
    request_bytes,
)

POD_UID = "c0000000-0000-4000-8000-000000000001"


class Unstarted(Cluster):
    """A Pod whose runner never starts; ``kubelet`` decides what deletion leaves."""

    def __init__(self, kubelet: str = "finished") -> None:
        super().__init__()
        self.fault_mode = "creation-no-container"
        self.kubelet = kubelet
        self.patches: list[Any] = []

    async def kubernetes(self, request: web.Request) -> web.StreamResponse:
        parts = request.path.split("/")
        if request.method == "POST" and parts[-1] == "pods":
            response = await super().kubernetes(request)
            if self.kubelet == "unscheduled":
                del self.pod["spec"]["nodeName"]
            if self.kubelet == "waiting":
                self.pod["status"]["containerStatuses"] = [
                    {
                        "name": "runner",
                        "ready": False,
                        "restartCount": 0,
                        "image": "example",
                        "imageID": "",
                        "state": {"waiting": {"reason": "ContainerCreating"}},
                    }
                ]
            return response
        if request.method == "DELETE" and parts[-2] == "pods":
            response = await super().kubernetes(request)
            if self.kubelet in {"finished", "waiting"}:
                self.pod["status"]["phase"] = "Failed"
            return response
        if request.method == "PATCH":
            key = (parts[-2], parts[-1])
            doc = self.objects[key]
            row = self.server._registry.get(OWNER, self.launch_id)  # type: ignore[union-attr]
            # The never-started fact is durable before the finalizer is removed.
            assert row.state == "retiring" and row.unstarted is not None
            assert row.unstarted.pod_uid == doc["metadata"]["uid"]
            self.events.append(("PATCH", request.path))
            self.patches.append(await request.json())
            removed = deepcopy(doc)
            del self.objects[key]
            return web.json_response(removed)
        return await super().kubernetes(request)


async def launched(http: httpx.AsyncClient, url: str, cluster: Cluster) -> tuple[str, str]:
    raw = request_bytes()
    launch_id = json.loads(raw)["launch_id"]
    cluster.launch_id = launch_id
    created = await http.post(url + ROUTE, content=raw)
    assert created.status_code == 202, created.json()
    return launch_id, created.json()["control_capability"]


async def retire(http: httpx.AsyncClient, url: str, launch_id: str, capability: str) -> Any:
    from curie_worker.commissioning_launch import CAPABILITY_HEADER

    return await http.post(
        url + ROUTE + "/" + launch_id + "/retire", headers={CAPABILITY_HEADER: capability}
    )


@pytest.mark.parametrize("kubelet", ["finished", "unscheduled", "waiting"])
def test_a_pod_deleted_before_its_runner_started_retires(tmp_path, kubelet):
    async def scenario() -> None:
        cluster = Unstarted(kubelet)
        async with mediator(tmp_path, cluster) as (server, control, url, root):
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                launch_id, capability = await launched(http, url, cluster)
                pod = object_names(launch_id)[0]
                await asyncio.sleep(0.2)
                pod_uid = cluster.objects[("pods", pod)]["metadata"]["uid"]
                retired = await retire(http, url, launch_id, capability)
                assert retired.status_code == 200, retired.json()
            row = server._registry.get(OWNER, launch_id)
            assert row.state == "retired" and row.termination is None
            assert row.unstarted is not None and row.unstarted.pod_uid == pod_uid
            assert row.unstarted.evidence == (
                "unscheduled" if kubelet == "unscheduled" else "terminal"
            )
            assert control.ownership[launch_id] == ("retired", pod_uid)
            # The only update is the finalizer-only patch on the exact UID.
            assert len(cluster.patches) == 1
            patch = cluster.patches[0]
            assert patch[0] == {"op": "test", "path": "/metadata/uid", "value": pod_uid}
            assert patch[2] == {"op": "test", "path": "/metadata/finalizers", "value": [FINALIZER]}
            assert patch[3] == {"op": "remove", "path": "/metadata/finalizers/0"}
            assert [p["op"] for p in patch] == ["test", "test", "test", "remove"]
            assert not [k for k in cluster.objects if k[0] in {"pods", "services", "secrets"}]

    asyncio.run(scenario())


def test_a_kubelet_that_has_not_finished_keeps_the_finalizer_until_it_does(tmp_path):
    async def scenario() -> None:
        cluster = Unstarted("pending")
        async with mediator(tmp_path, cluster) as (server, control, url, root):
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                launch_id, capability = await launched(http, url, cluster)
                await asyncio.sleep(0.2)
                pending = await retire(http, url, launch_id, capability)
                assert pending.status_code == 202, pending.json()
                pod = cluster.objects[("pods", object_names(launch_id)[0])]
                assert pod["metadata"]["finalizers"] == [FINALIZER]
                assert pod["metadata"].get("deletionTimestamp")
                row = server._registry.get(OWNER, launch_id)
                assert row.state == "retiring" and row.unstarted is None
                assert control.ownership[launch_id] == ("retiring", pod["metadata"]["uid"])
                assert cluster.patches == []
                # Once the kubelet reports the deleted Pod terminal, the retry retires it.
                pod["status"]["phase"] = "Failed"
                retired = await retire(http, url, launch_id, capability)
                assert retired.status_code == 200, retired.json()
            assert control.ownership[launch_id][0] == "retired"

    asyncio.run(scenario())


@pytest.mark.parametrize("change", ["container-id", "restarted", "absent", "replaced"])
def test_no_container_identity_absence_or_replacement_proves_never_started(tmp_path, change):
    async def scenario() -> None:
        cluster = Unstarted("finished" if change in {"container-id", "restarted"} else "pending")
        async with mediator(tmp_path, cluster) as (server, control, url, root):
            async with httpx.AsyncClient(verify=context(root), trust_env=False) as http:
                launch_id, capability = await launched(http, url, cluster)
                await asyncio.sleep(0.2)
                key = ("pods", object_names(launch_id)[0])
                status = {
                    "name": "runner",
                    "ready": False,
                    "restartCount": 0,
                    "state": {"waiting": {"reason": "ContainerCreating"}},
                }
                if change == "container-id":
                    status["containerID"] = "containerd://" + "b" * 64
                    cluster.pod["status"]["containerStatuses"] = [status]
                elif change == "restarted":
                    status["restartCount"] = 1
                    cluster.pod["status"]["containerStatuses"] = [status]
                first = await retire(http, url, launch_id, capability)
                assert first.status_code == 202, first.json()
                if change == "absent":
                    del cluster.objects[key]
                elif change == "replaced":
                    cluster.objects[key]["metadata"]["uid"] = POD_UID
                    cluster.objects[key]["status"]["phase"] = "Failed"
                again = await retire(http, url, launch_id, capability)
                assert again.status_code == 202, again.json()
            row = server._registry.get(OWNER, launch_id)
            assert row.state == "retiring" and row.unstarted is None
            assert cluster.patches == []
            assert control.ownership[launch_id][0] == "retiring"

    asyncio.run(scenario())


@pytest.mark.parametrize("evidence", ["", "Pending", "running", None, 7])
def test_the_never_started_observation_is_closed(evidence):
    with pytest.raises(RegistryError):
        cleanup.UnstartedObservation(POD_UID, "7", evidence)  # type: ignore[arg-type]

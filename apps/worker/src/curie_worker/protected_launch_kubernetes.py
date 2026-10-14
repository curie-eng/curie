"""Issuer-owned explicit Kubernetes HTTPS control. No kubeconfig or ambient credentials.

@spec PROTECTED-HOOK-LANE-2 PROTECTED-HOOK-LANE-8.
Transport observations and parsers do not qualify a deployment.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import re
import ssl
from dataclasses import dataclass, field
from typing import Any, NoReturn, cast
from urllib.parse import quote, urlsplit

import httpx

from .protected_kubernetes_projection import Projection, extract_pod_projection
from .protected_launch_records import IssuerPodBinding, canonical_json
from .protected_launch_registry import TerminationObservation
from .protected_substrate_facts import parse_kubernetes_runner_facts
from .sandbox.types import SandboxHandle

FINALIZER = "curietech.ai/protected-launch"
OBSERVATION_CAP = 1048576


class LaunchControlUnavailable(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Protected launch control unavailable")


def unavailable() -> NoReturn:
    error = LaunchControlUnavailable()
    try:
        raise error from None
    finally:
        error.__context__ = None
        error.__cause__ = None


def require(condition: bool) -> None:
    if not condition:
        unavailable()


def present[T](value: T | None) -> T:
    if value is None:
        unavailable()
    return value


def _name(value: str) -> str:
    require(
        type(value) is str
        and re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", value) is not None
    )
    return value


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for k, v in items:
        require(k not in result)
        result[k] = v
    return result


def decode_observation(raw: bytes) -> dict[str, Any]:
    require(0 < len(raw) <= OBSERVATION_CAP)
    value = json.loads(raw, object_pairs_hook=_pairs, parse_constant=lambda _: require(False))
    require(type(value) is dict)
    return cast(dict[str, Any], value)


@dataclass(frozen=True, slots=True, repr=False)
class KubernetesTLS:
    origin: str
    ca_file: str
    client_certificate: str
    client_key: str = field(repr=False)
    bearer: str = field(repr=False)

    def context(self) -> ssl.SSLContext:
        url = urlsplit(self.origin)
        require(
            url.scheme == "https"
            and bool(url.hostname)
            and url.path in ("", "/")
            and not url.username
            and not url.password
            and not url.query
            and not url.fragment
        )
        require(bool(self.bearer) and "\r" not in self.bearer and "\n" not in self.bearer)
        context = ssl.create_default_context(cafile=self.ca_file)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(self.client_certificate, self.client_key)
        return context


class KubernetesLaunchAPI:
    """Actual bounded HTTPS API; no redirects, proxy, retries or generic exposed path."""

    def __init__(self, tls: KubernetesTLS, *, authority_domain_id: str) -> None:
        self.authority_domain_id = authority_domain_id
        self._origin = tls.origin.rstrip("/")
        self._client = httpx.AsyncClient(
            verify=tls.context(),
            trust_env=False,
            follow_redirects=False,
            timeout=5,
            headers={"Authorization": "Bearer " + tls.bearer},
            transport=None,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        value: Any = None,
        *,
        patch: bool = False,
        absent: bool = False,
    ) -> dict[str, Any] | None:
        try:
            async with asyncio.timeout(5):
                headers = {
                    "Content-Type": "application/json-patch+json" if patch else "application/json"
                }
                async with self._client.stream(
                    method,
                    self._origin + path,
                    headers=headers,
                    content=None if value is None else canonical_json(value),
                ) as response:
                    if absent and response.status_code == 404:
                        return None
                    require(200 <= response.status_code < 300)
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        require(len(body) + len(chunk) <= OBSERVATION_CAP)
                        body.extend(chunk)
                    return decode_observation(bytes(body))
        except Exception:  # noqa: BLE001 - fixed fail-closed diagnostic
            unavailable()

    def _resource(self, namespace: str, kind: str, name: str | None = None) -> str:
        path = "/api/v1/namespaces/" + _name(namespace) + "/" + kind
        return path if name is None else path + "/" + _name(name)

    async def create(self, namespace: str, kind: str, document: dict[str, Any]) -> dict[str, Any]:
        require(kind in {"pods", "services", "secrets"})
        result = await self._request("POST", self._resource(namespace, kind), document)
        require(result is not None)
        return result  # type: ignore[return-value]

    async def read(self, namespace: str, kind: str, name: str) -> dict[str, Any] | None:
        require(kind in {"pods", "services", "secrets"})
        return await self._request("GET", self._resource(namespace, kind, name), absent=True)

    async def node(self, name: str) -> dict[str, Any]:
        # Node names may be DNS subdomains; never URL paths.
        require(type(name) is str and re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", name) is not None)
        result = await self._request("GET", "/api/v1/nodes/" + quote(name, safe=""))
        require(result is not None)
        return result  # type: ignore[return-value]

    async def endpoints(self, namespace: str, service_name: str) -> dict[str, Any]:
        result = await self._request(
            "GET",
            "/apis/discovery.k8s.io/v1/namespaces/"
            + _name(namespace)
            + "/endpointslices?labelSelector="
            + quote("kubernetes.io/service-name=" + _name(service_name), safe=""),
        )
        require(result is not None)
        return result  # type: ignore[return-value]

    async def delete(self, namespace: str, kind: str, name: str, uid: str) -> None:
        require(kind in {"pods", "services", "secrets"})
        await self._request(
            "DELETE",
            self._resource(namespace, kind, name),
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "preconditions": {"uid": uid},
                "gracePeriodSeconds": 5,
            },
            absent=True,
        )

    async def remove_finalizer(self, namespace: str, name: str, uid: str, version: str) -> None:
        # JSON Patch tests keep UID/RV and finalizer membership atomic, touching no other metadata.
        await self._request(
            "PATCH",
            self._resource(namespace, "pods", name),
            [
                {"op": "test", "path": "/metadata/uid", "value": uid},
                {"op": "test", "path": "/metadata/resourceVersion", "value": version},
                {"op": "test", "path": "/metadata/finalizers", "value": [FINALIZER]},
                {"op": "remove", "path": "/metadata/finalizers/0"},
            ],
            patch=True,
        )


def object_identity(document: Any, *, namespace: str, name: str) -> tuple[str, str]:
    try:
        require(type(document) is dict and type(document.get("metadata")) is dict)
        m = document["metadata"]
        require(m.get("namespace") == namespace and m.get("name") == name)
        uid, version = m.get("uid"), m.get("resourceVersion")
        require(type(uid) is str and type(version) is str and 0 < len(version) <= 256)
        from uuid import UUID

        require(str(UUID(uid)) == uid)
        return uid, version
    except Exception:  # noqa: BLE001 - bounded ownership refusal
        unavailable()


def verify_secret(
    document: Any, *, namespace: str, name: str, uid: str, version: str, source: dict[str, str]
) -> None:
    require(object_identity(document, namespace=namespace, name=name) == (uid, version))
    require(document.get("immutable") is True and document.get("type") == "Opaque")
    require(document["metadata"].get("deletionTimestamp") is None)
    data = document.get("data")
    require(type(data) is dict and data.keys() == source.keys() and "stringData" not in document)
    for key, value in source.items():
        actual = base64.b64decode(data[key], validate=True)
        require(hmac.compare_digest(actual, value.encode("utf-8")))


def termination_observation(
    pod: Any, *, namespace: str, pod_name: str, pod_uid: str, container_id: str
) -> TerminationObservation:
    try:
        uid, version = object_identity(pod, namespace=namespace, name=pod_name)
        require(uid == pod_uid and pod["metadata"].get("finalizers") == [FINALIZER])
        require(pod["metadata"].get("deletionTimestamp") is not None)
        statuses = pod["status"]["containerStatuses"]
        require(type(statuses) is list and len(statuses) == 1)
        c = statuses[0]
        require(
            c["name"] == "runner"
            and c["containerID"] == container_id
            and type(c["restartCount"]) is int
            and c["restartCount"] == 0
            and set(c["state"]) == {"terminated"}
        )
        t = c["state"]["terminated"]
        require(t.get("containerID") == container_id and bool(t.get("startedAt")))
        return TerminationObservation(
            pod_uid, container_id, version, t["exitCode"], t["finishedAt"]
        )
    except Exception:  # noqa: BLE001 - fixed fail-closed diagnostic
        unavailable()


@dataclass(frozen=True, slots=True, repr=False)
class DirectPodObservation:
    container_id: str
    started_at: str
    node_uid: str
    pod_ip: str
    platform: dict[str, Any]


async def observe_direct_pod(
    api: KubernetesLaunchAPI,
    binding: IssuerPodBinding,
    *,
    launch_id: str,
    projection: Projection,
    measured_platform: dict[str, Any],
    secret_name: str,
    secret_uid: str,
    secret_version: str,
    source: dict[str, str],
    session_id: str,
    history_ref: str,
    container_id: str | None = None,
    node_uid: str | None = None,
) -> DirectPodObservation:
    """Authenticate exact direct Pod/Secret/Node/Service/EndpointSlice assignment."""
    try:
        pod = await api.read(binding.namespace, "pods", binding.pod_name)
        secret = await api.read(binding.namespace, "secrets", secret_name)
        verify_secret(
            secret,
            namespace=binding.namespace,
            name=secret_name,
            uid=secret_uid,
            version=secret_version,
            source=source,
        )
        extract_pod_projection(
            pod,
            projection=projection,
            launch_id=launch_id,
            pod_name=binding.pod_name,
            pod_uid=binding.pod_uid,
            secret_name=secret_name,
            session_id=session_id,
            history_ref=history_ref,
        )
        require(pod is not None)
        pod = present(pod)
        status = pod["status"]
        statuses = status["containerStatuses"]
        require(type(statuses) is list and len(statuses) == 1)
        c = statuses[0]
        cid = container_id or c["containerID"]
        # Reuse the existing factual parser without forging a Sandbox owner.
        handle = SandboxHandle(
            thread_key="issuer-observation",
            claim_name="issuer-observation",
            port=8080,
            session_id=session_id,
            namespace=binding.namespace,
            sandbox_name=binding.pod_name,
            service_fqdn="issuer-observation",
            token=binding.runner_token,
        )
        facts = parse_kubernetes_runner_facts(
            pod, handle=handle, pod_uid=binding.pod_uid, container_id=cid
        )
        require(facts.image_id == measured_platform["observed_image_id"])
        require(facts.declared_image == measured_platform["pinned_image_reference"])
        started = c["state"]["running"]["startedAt"]
        require(type(started) is str)
        node_name = pod["spec"]["nodeName"]
        node = await api.node(node_name)
        nm = node["metadata"]
        require(nm["name"] == node_name and nm.get("deletionTimestamp") is None)
        from uuid import UUID

        require(str(UUID(nm["uid"])) == nm["uid"] and (node_uid is None or node_uid == nm["uid"]))
        info = node["status"]["nodeInfo"]
        require(
            info["operatingSystem"] == measured_platform["os"]
            and info["architecture"] == measured_platform["architecture"]
            and info["containerRuntimeVersion"] == measured_platform["runtime_profile"]
            and measured_platform["variant"] is None
            and measured_platform["image_config_id"] is None
        )
        service = await api.read(binding.namespace, "services", binding.service_name)
        require(
            object_identity(service, namespace=binding.namespace, name=binding.service_name)[0]
            == binding.service_uid
        )
        require(service is not None and service["metadata"].get("deletionTimestamp") is None)
        service = present(service)
        s = service["spec"]
        require(
            s["clusterIP"] == "None"
            and s["type"] == "ClusterIP"
            and s["selector"] == {FINALIZER: launch_id}
            and not s.get("externalIPs")
            and not s.get("externalName")
        )
        require(
            s["ports"] == [{"name": "http", "port": 8080, "targetPort": 8080, "protocol": "TCP"}]
        )
        slices = await api.endpoints(binding.namespace, binding.service_name)
        require(not slices.get("metadata", {}).get("continue") and len(slices["items"]) == 1)
        es = slices["items"][0]
        require(
            es["metadata"]["namespace"] == binding.namespace
            and es["metadata"].get("deletionTimestamp") is None
        )
        owners = es["metadata"]["ownerReferences"]
        require(
            len(owners) == 1
            and owners[0]["kind"] == "Service"
            and owners[0]["uid"] == binding.service_uid
            and owners[0]["name"] == binding.service_name
            and owners[0]["controller"] is True
        )
        require(es["ports"] == [{"name": "http", "port": 8080, "protocol": "TCP"}])
        require(len(es["endpoints"]) == 1)
        endpoint = es["endpoints"][0]
        target = endpoint["targetRef"]
        require(
            endpoint["conditions"].get("ready") is True
            and endpoint["conditions"].get("terminating") is not True
            and endpoint["addresses"] == [status["podIP"]]
            and target["kind"] == "Pod"
            and target["namespace"] == binding.namespace
            and target["name"] == binding.pod_name
            and target["uid"] == binding.pod_uid
        )
        return DirectPodObservation(
            cid, started, nm["uid"], status["podIP"], dict(measured_platform)
        )
    except Exception:  # noqa: BLE001 - fixed fail-closed diagnostic
        unavailable()


def secret_document(namespace: str, name: str, source: dict[str, str]) -> dict[str, Any]:
    require(source.keys() == {"runner-token", "history-token", "model-credential"})
    require(all(type(v) is str and v and len(v.encode()) <= 8192 for v in source.values()))
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"namespace": namespace, "name": name},
        "type": "Opaque",
        "immutable": True,
        "data": {k: base64.b64encode(v.encode()).decode("ascii") for k, v in source.items()},
    }


def service_document(namespace: str, name: str, launch_id: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"namespace": namespace, "name": name},
        "spec": {
            "type": "ClusterIP",
            "clusterIP": "None",
            "selector": {FINALIZER: launch_id},
            "ports": [{"name": "http", "port": 8080, "targetPort": 8080, "protocol": "TCP"}],
        },
    }


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

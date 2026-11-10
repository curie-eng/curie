"""Read selected control planes and fence observations to a cold-launch receipt.

Receipts must come from an independently owned cold launcher. Constructing this
value does not confer launch authority. These readers establish factual identity
comparisons only: qualified image/platform mapping, qualified launch-document
projection, preventive isolation and current admission remain separate guards.
No raw snapshots, environment or credentials leave the readers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import ssl
from dataclasses import dataclass
from typing import Any, NoReturn, Protocol
from urllib.parse import quote, urlsplit

import httpx
from aci_protocol import BootEnv

from .protected_substrate_facts import (
    SubstrateFactsUnavailable,
    parse_kubernetes_runner_facts,
)
from .sandbox.types import SandboxHandle

_MAX_RESPONSE_BYTES = 1_048_576
_TOKEN_ENV = BootEnv.env_key("runner_token")
_DOCKER_ID = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_ID = re.compile(r"^sha256:[0-9a-f]{64}$")
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")


@dataclass(frozen=True, slots=True, repr=False)
class ProtectedLaunchReceipt:
    event_id: str
    execution_session_key: str
    runtime_generation: int
    manifest_digest: str
    handle: SandboxHandle
    authority_domain_id: str
    launch_identity: str
    launch_config_sha256: str
    workload_uid: str
    container_id: str
    started_at: str
    token_sha256: str
    sandbox_uid: str | None = None
    service_name: str | None = None
    service_uid: str | None = None


@dataclass(frozen=True, slots=True, repr=False)
class ProtectedRunnerObservation:
    authority_domain_id: str
    substrate: str
    workload_uid: str
    container_id: str
    started_at: str
    declared_image: str
    image_id: str
    image_config_id: str | None
    repo_digests: tuple[str, ...]
    platform_os: str
    platform_architecture: str
    platform_variant: str | None
    runtime_profile: str | None
    node_uid: str | None
    token_sha256: str
    # Raw document hash, deliberately not a qualified launch-config projection.
    # It must never be compared to launch_config_sha256 to invent qualification.
    launch_document_sha256: str


class DockerControlPlane(Protocol):
    authority_domain_id: str
    endpoint_host: str

    async def inspect_container(self, container_id: str) -> object: ...
    async def inspect_image(self, image_id: str) -> object: ...


class KubernetesControlPlane(Protocol):
    authority_domain_id: str

    async def read_pod(self, namespace: str, name: str) -> object: ...
    async def read_node(self, name: str) -> object: ...
    async def read_service(self, namespace: str, name: str) -> object: ...
    async def list_endpoint_slices(self, namespace: str, service_name: str) -> object: ...


def _unavailable() -> NoReturn:
    # Python can attach an already-active caller exception even when a source
    # catch scope has ended. Clear the actual chain during propagation, rather
    # than relying only on traceback display suppression from `from None`.
    error = SubstrateFactsUnavailable()
    try:
        raise error from None
    finally:
        error.__context__ = None
        error.__cause__ = None


def _require(condition: bool) -> None:
    if not condition:
        _unavailable()


def _object(value: object) -> dict[str, Any]:
    _require(type(value) is dict)
    return value  # type: ignore[return-value]


def _list(value: object) -> list[Any]:
    _require(type(value) is list)
    return value  # type: ignore[return-value]


def _text(value: object) -> str:
    _require(type(value) is str)
    _require(bool(value) and len(value) <= 2048)  # type: ignore[arg-type]
    text = str(value)
    _require(not any(ord(ch) <= 0x20 for ch in text))
    return text


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _token_hash(token: object) -> str:
    return hashlib.sha256(_text(token).encode("utf-8")).hexdigest()


def _bind(receipt: ProtectedLaunchReceipt, handle: SandboxHandle, domain: str) -> None:
    _require(type(receipt) is ProtectedLaunchReceipt and type(handle) is SandboxHandle)
    _require(handle == receipt.handle and domain == receipt.authority_domain_id)
    _require(handle.thread_key == receipt.execution_session_key)
    _require(_token_hash(handle.token) == receipt.token_sha256)
    for value in (
        receipt.event_id,
        receipt.execution_session_key,
        receipt.authority_domain_id,
        receipt.launch_identity,
        receipt.workload_uid,
        receipt.container_id,
        receipt.started_at,
    ):
        _text(value)
    _require(type(receipt.runtime_generation) is int and receipt.runtime_generation > 0)
    for value in (receipt.manifest_digest, receipt.launch_config_sha256, receipt.token_sha256):
        _require(_DOCKER_ID.fullmatch(value) is not None)
    _require(type(handle.port) is int and 1 <= handle.port <= 65535)


def _owner(metadata: dict[str, Any], kind: str, name: str, uid: str) -> None:
    owners = _list(metadata.get("ownerReferences"))
    controllers = [_object(value) for value in owners if _object(value).get("controller") is True]
    _require(len(controllers) == 1)
    owner = controllers[0]
    _require(owner.get("kind") == kind and owner.get("name") == name and owner.get("uid") == uid)


class DockerSubstrateReader:
    """Own one explicit Engine client; never discover an ambient Docker daemon."""

    def __init__(self, control_plane: DockerControlPlane) -> None:
        self._plane = control_plane
        self._domain = _text(control_plane.authority_domain_id)

    async def observe(
        self,
        receipt: ProtectedLaunchReceipt,
        *,
        handle: SandboxHandle,
    ) -> ProtectedRunnerObservation:
        try:
            _bind(receipt, handle, self._domain)
            _require(self._plane.authority_domain_id == self._domain)
            _require(_DOCKER_ID.fullmatch(receipt.container_id) is not None)
            _require(receipt.workload_uid == receipt.container_id)
            container = _object(await self._plane.inspect_container(receipt.container_id))
            # Engine ContainerInspect: Id, Image (config digest), State.StartedAt,
            # RestartCount and NetworkSettings.Ports. ImageInspect has platform.
            # https://docs.docker.com/reference/api/engine/version/v1.47/
            _require(container.get("Id") == receipt.container_id)
            _require(container.get("Name") == "/" + handle.sandbox_name)
            state, config = _object(container.get("State")), _object(container.get("Config"))
            _require(
                state.get("Running") is True
                and state.get("Paused") is False
                and state.get("Restarting") is False
                and state.get("StartedAt") == receipt.started_at
            )
            _require(type(container.get("RestartCount")) is int and container["RestartCount"] == 0)
            values = [_text(value).partition("=") for value in _list(config.get("Env"))]
            tokens = [value for key, sep, value in values if key == _TOKEN_ENV and sep]
            _require(len(tokens) == 1 and _token_hash(tokens[0]) == receipt.token_sha256)
            # This slice supports only an exact loopback published endpoint, not
            # remote host reachability or a container/network-IP inference.
            _require(self._plane.endpoint_host == "127.0.0.1")
            _require(handle.service_fqdn == "127.0.0.1")
            ports = _object(_object(container.get("NetworkSettings")).get("Ports"))
            bindings = _list(ports.get("8080/tcp"))
            _require(len(bindings) == 1)
            binding = _object(bindings[0])
            _require(
                binding.get("HostIp") == "127.0.0.1" and binding.get("HostPort") == str(handle.port)
            )
            image_id = _text(container.get("Image"))
            _require(_IMAGE_ID.fullmatch(image_id) is not None)
            image = _object(await self._plane.inspect_image(image_id))
            _require(image.get("Id") == image_id)
            digests = tuple(_text(value) for value in _list(image.get("RepoDigests")))
            return ProtectedRunnerObservation(
                self._domain,
                "docker",
                receipt.workload_uid,
                receipt.container_id,
                receipt.started_at,
                _text(config.get("Image")),
                image_id,
                image_id,
                digests,
                _text(image.get("Os")),
                _text(image.get("Architecture")),
                _text(image["Variant"]) if image.get("Variant") else None,
                None,
                None,
                receipt.token_sha256,
                _hash({"Config": config, "HostConfig": _object(container.get("HostConfig"))}),
            )
        except Exception:  # noqa: BLE001 - redact external control-plane failures
            pass
        _unavailable()


class KubernetesSubstrateReader:
    """Own selected Pod/Node/Service APIs; do not load kubeconfig or use labels as UID proof."""

    def __init__(self, control_plane: KubernetesControlPlane) -> None:
        self._plane = control_plane
        self._domain = _text(control_plane.authority_domain_id)

    async def observe(
        self,
        receipt: ProtectedLaunchReceipt,
        *,
        handle: SandboxHandle,
    ) -> ProtectedRunnerObservation:
        try:
            _bind(receipt, handle, self._domain)
            _require(self._plane.authority_domain_id == self._domain)
            sandbox_uid = _text(receipt.sandbox_uid)
            service_name, service_uid = _text(receipt.service_name), _text(receipt.service_uid)
            pod = _object(await self._plane.read_pod(handle.namespace, handle.sandbox_name))
            facts = parse_kubernetes_runner_facts(
                pod,
                handle=handle,
                pod_uid=receipt.workload_uid,
                container_id=receipt.container_id,
            )
            _owner(_object(pod.get("metadata")), "Sandbox", handle.sandbox_name, sandbox_uid)
            spec, status = _object(pod.get("spec")), _object(pod.get("status"))
            running = next(
                _object(value)
                for value in _list(status.get("containerStatuses"))
                if _object(value).get("name") == "runner"
            )
            _require(
                _object(_object(running.get("state")).get("running")).get("startedAt")
                == receipt.started_at
            )
            runner = next(
                _object(value)
                for value in _list(spec.get("containers"))
                if _object(value).get("name") == "runner"
            )
            tokens = [
                _object(value)
                for value in _list(runner.get("env"))
                if _object(value).get("name") == _TOKEN_ENV
            ]
            # A SecretKeyRef alone proves no value/assignment hash. Current
            # protected templates use it: they are unsupported until an
            # independently authenticated assignment proof is implemented.
            _require(len(tokens) == 1 and "valueFrom" not in tokens[0])
            _require(_token_hash(tokens[0].get("value")) == receipt.token_sha256)
            _require(handle.service_fqdn == f"{service_name}.{handle.namespace}.svc.cluster.local")
            service = _object(await self._plane.read_service(handle.namespace, service_name))
            metadata = _object(service.get("metadata"))
            _require(
                metadata.get("name") == service_name
                and metadata.get("namespace") == handle.namespace
                and metadata.get("uid") == service_uid
                and metadata.get("deletionTimestamp") is None
            )
            service_spec = _object(service.get("spec"))
            _require(
                service_spec.get("type") == "ClusterIP"
                and service_spec.get("clusterIP") == "None"
                and "externalName" not in service_spec
                and not service_spec.get("externalIPs")
            )
            service_ports = _list(service_spec.get("ports"))
            _require(len(service_ports) == 1)
            port = _object(service_ports[0])
            _require(
                port.get("port") == handle.port
                and port.get("targetPort") == 8080
                and port.get("protocol") == "TCP"
            )
            slices = _object(await self._plane.list_endpoint_slices(handle.namespace, service_name))
            _require(not _object(slices.get("metadata", {})).get("continue"))
            items = _list(slices.get("items"))
            _require(len(items) == 1)
            item = _object(items[0])
            slice_metadata = _object(item.get("metadata"))
            _require(
                slice_metadata.get("namespace") == handle.namespace
                and slice_metadata.get("deletionTimestamp") is None
            )
            _owner(slice_metadata, "Service", service_name, service_uid)
            slice_ports = _list(item.get("ports"))
            # Headless DNS resolves directly to endpoints; there is no VIP
            # proxy or service-port translation on this path.
            # https://kubernetes.io/docs/concepts/services-networking/service/#headless-services
            _require(
                len(slice_ports) == 1
                and _object(slice_ports[0]).get("port") == 8080
                and _object(slice_ports[0]).get("port") == handle.port
                and _object(slice_ports[0]).get("protocol") == "TCP"
            )
            endpoints = _list(item.get("endpoints"))
            _require(len(endpoints) == 1)
            endpoint = _object(endpoints[0])
            conditions, target = (
                _object(endpoint.get("conditions")),
                _object(endpoint.get("targetRef")),
            )
            _require(conditions.get("ready") is True and conditions.get("terminating") is not True)
            _require(
                target.get("kind") == "Pod"
                and target.get("namespace") == handle.namespace
                and target.get("name") == handle.sandbox_name
                and target.get("uid") == receipt.workload_uid
            )
            _require(endpoint.get("addresses") == [_text(status.get("podIP"))])
            # Node.status.nodeInfo is authoritative platform/runtime reporting;
            # no kubernetes.io/arch label is promoted to measured qualification.
            # Pod, Node, Service, EndpointSlice API references:
            # https://kubernetes.io/docs/reference/kubernetes-api/
            node_name = _text(spec.get("nodeName"))
            node = _object(await self._plane.read_node(node_name))
            node_metadata = _object(node.get("metadata"))
            _require(
                node_metadata.get("name") == node_name
                and node_metadata.get("deletionTimestamp") is None
            )
            info = _object(_object(node.get("status")).get("nodeInfo"))
            return ProtectedRunnerObservation(
                self._domain,
                "kubernetes",
                facts.pod_uid,
                facts.container_id,
                receipt.started_at,
                facts.declared_image,
                facts.image_id,
                None,
                (),
                _text(info.get("operatingSystem")),
                _text(info.get("architecture")),
                None,
                _text(info.get("containerRuntimeVersion")),
                _text(node_metadata.get("uid")),
                receipt.token_sha256,
                _hash(spec),
            )
        except Exception:  # noqa: BLE001 - redact external control-plane failures
            pass
        _unavailable()


class _AuthenticatedJSONAPI:
    """Private bounded HTTPS transport: no environment proxies, redirects or retries."""

    def __init__(
        self,
        *,
        authority_domain_id: str,
        endpoint: str,
        tls_context: ssl.SSLContext,
        headers: dict[str, str] | None = None,
    ) -> None:
        try:
            parsed = urlsplit(endpoint)
            _require(
                parsed.scheme == "https"
                and parsed.hostname is not None
                and parsed.username is None
                and parsed.password is None
                and parsed.path in {"", "/"}
                and not parsed.query
                and not parsed.fragment
            )
            _require(tls_context.verify_mode == ssl.CERT_REQUIRED and tls_context.check_hostname)
            self.authority_domain_id = _text(authority_domain_id)
            self.endpoint_host = _text(parsed.hostname)
            self._client = httpx.AsyncClient(
                base_url=endpoint.rstrip("/"),
                verify=tls_context,
                headers=headers,
                timeout=httpx.Timeout(5.0),
                follow_redirects=False,
                trust_env=False,
            )
            return
        except Exception:  # noqa: BLE001 - redact external control-plane failures
            pass
        _unavailable()

    async def _get(self, path: str, *, params: dict[str, str] | None = None) -> object:
        try:
            # HTTPX's per-operation timeouts cannot bound slow header progress.
            # This cancellation scope owns connect, headers, body and stream cleanup.
            async with asyncio.timeout(5.0):
                async with self._client.stream(
                    "GET",
                    path,
                    params=params,
                    headers={"Accept-Encoding": "identity"},
                ) as response:
                    _require(response.status_code == 200)
                    _require(response.headers.get("Content-Encoding", "identity") == "identity")
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        _require(len(data) + len(chunk) <= _MAX_RESPONSE_BYTES)
                        data.extend(chunk)
                    return json.loads(data)
        except Exception:  # noqa: BLE001 - detach secret-bearing external failures
            pass
        _unavailable()

    async def aclose(self) -> None:
        try:
            await self._client.aclose()
            return
        except Exception:  # noqa: BLE001 - detach external transport failures
            pass
        _unavailable()


class DockerEngineControlPlane(_AuthenticatedJSONAPI):
    """Explicit authenticated Engine HTTPS endpoint; no Docker socket discovery.

    The provisioner supplies a verifying SSLContext with its client certificate
    loaded for mTLS. Selecting the context/domain is provisioner composition, not
    evidence of preventive daemon policy or receipt ownership.
    """

    def __init__(
        self,
        *,
        authority_domain_id: str,
        endpoint: str,
        tls_context: ssl.SSLContext,
        client_certificate_file: str | None = None,
        client_key_file: str | None = None,
    ) -> None:
        try:
            if client_certificate_file is None or client_key_file is None:
                _unavailable()
            tls_context.load_cert_chain(client_certificate_file, client_key_file)
            super().__init__(
                authority_domain_id=authority_domain_id,
                endpoint=endpoint,
                tls_context=tls_context,
            )
            return
        except Exception:  # noqa: BLE001 - redact certificate paths and external failures
            pass
        _unavailable()

    async def inspect_container(self, container_id: str) -> object:
        _require(_DOCKER_ID.fullmatch(container_id) is not None)
        return await self._get(f"/v1.47/containers/{container_id}/json")

    async def inspect_image(self, image_id: str) -> object:
        _require(_IMAGE_ID.fullmatch(image_id) is not None)
        return await self._get(f"/v1.47/images/{quote(image_id, safe='')}/json")


class KubernetesAPIControlPlane(_AuthenticatedJSONAPI):
    """Selected CA-verified API server with an explicit read credential, never kubeconfig."""

    def __init__(
        self,
        *,
        authority_domain_id: str,
        endpoint: str,
        tls_context: ssl.SSLContext,
        token: str,
    ) -> None:
        super().__init__(
            authority_domain_id=authority_domain_id,
            endpoint=endpoint,
            tls_context=tls_context,
            headers={"Authorization": "Bearer " + _text(token)},
        )

    async def read_pod(self, namespace: str, name: str) -> object:
        return await self._get(f"/api/v1/namespaces/{_name(namespace)}/pods/{_name(name)}")

    async def read_node(self, name: str) -> object:
        return await self._get(f"/api/v1/nodes/{_name(name)}")

    async def read_service(self, namespace: str, name: str) -> object:
        return await self._get(f"/api/v1/namespaces/{_name(namespace)}/services/{_name(name)}")

    async def list_endpoint_slices(self, namespace: str, service_name: str) -> object:
        return await self._get(
            f"/apis/discovery.k8s.io/v1/namespaces/{_name(namespace)}/endpointslices",
            params={
                "labelSelector": "kubernetes.io/service-name=" + _name(service_name),
                "limit": "16",
            },
        )


def _name(value: str) -> str:
    _require(
        type(value) is str
        and all(_DNS_LABEL.fullmatch(part) is not None for part in value.split("."))
    )
    _require(0 < len(value) <= 253)
    return value

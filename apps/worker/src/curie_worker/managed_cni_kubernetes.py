"""The measuring identity's bounded Kubernetes API client, ADR 0238.

@spec MANAGED-CNI-5. One explicit API origin, one CA file and one short-lived
bearer token for the measuring ServiceAccount. It never reads a kubeconfig, the
environment or ambient credentials, follows no redirect and uses no proxy.

The client exposes only the requests the probe kit makes, and each one maps to
a rule of the clause 5 manifest: probe pods, their logs and server-side dry
runs in the probe, protected and release namespaces; SubjectAccessReviews; and
cluster reads of Nodes, RBAC bindings, NetworkPolicies and the provider policy
collections. It has no exec, attach, port-forward, Secret or RBAC write path.
"""

from __future__ import annotations

import asyncio
import json
import re
import ssl
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote, urlsplit

import httpx

from .managed_cni_cases import ResourceAttributes
from .managed_cni_probe_kit import ProbeKitRefused, refuse, require
from .managed_cni_probe_programs import LOG_CAP

_NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_PROVIDER = re.compile(r"/apis/[a-z0-9.-]+/v[0-9a-z]+/[a-z0-9]+")
_LISTS = {
    "nodes": "/api/v1/nodes",
    "namespaces": "/api/v1/namespaces",  # clause 5 (the least-privilege probe list)
    "pods": "/api/v1/pods",  # cluster-wide, clause 5 (case 7 running_node_capability)
    "rolebindings": "/apis/rbac.authorization.k8s.io/v1/rolebindings",
    "clusterrolebindings": "/apis/rbac.authorization.k8s.io/v1/clusterrolebindings",
}
_TIMEOUT_S = 15


@dataclass(frozen=True, slots=True, repr=False)
class MeasuringCredentials:
    origin: str
    ca_file: str
    token: str = field(repr=False)

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
        require(bool(self.token) and "\r" not in self.token and "\n" not in self.token)
        context = ssl.create_default_context(cafile=self.ca_file)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        return context


def _name(value: str) -> str:
    require(type(value) is str and _NAME.fullmatch(value) is not None)
    return value


class MeasuringIdentityAPI:
    """Implements ``ProbeCluster`` with the clause 5 rights and nothing else."""

    def __init__(
        self,
        credentials: MeasuringCredentials,
        *,
        namespaces: Sequence[str],
        policy_namespaces: Sequence[str] = (),
    ) -> None:
        # The namespaces the probe kit may create pods in: probe, protected and release.
        self._namespaces = frozenset(_name(item) for item in namespaces)
        # Clause 5: NetworkPolicy writes are allowed only in the probe namespaces.
        self._policy_namespaces = frozenset(_name(item) for item in policy_namespaces)
        self._origin = credentials.origin.rstrip("/")
        self._client = httpx.AsyncClient(
            verify=credentials.context(),
            trust_env=False,
            follow_redirects=False,
            timeout=_TIMEOUT_S,
            headers={"Authorization": "Bearer " + credentials.token},
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _send(self, method: str, path: str, value: Any = None) -> tuple[int, bytes]:
        try:
            async with asyncio.timeout(_TIMEOUT_S):
                async with self._client.stream(
                    method,
                    self._origin + path,
                    headers={"Content-Type": "application/json"},
                    content=None if value is None else json.dumps(value).encode("utf-8"),
                ) as response:
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        require(len(body) + len(chunk) <= LOG_CAP)
                        body.extend(chunk)
                    return response.status_code, bytes(body)
        except ProbeKitRefused:
            raise
        except Exception:  # noqa: BLE001 - fixed fail-closed diagnostic
            refuse()

    async def _json(self, method: str, path: str, value: Any = None) -> dict[str, Any] | None:
        status, body = await self._send(method, path, value)
        if status == 404:
            return None
        require(200 <= status < 300)
        try:
            document = json.loads(body)
        except ValueError:
            refuse()
        require(type(document) is dict)
        return document  # type: ignore[no-any-return]

    def _pods(self, namespace: str, name: str | None = None) -> str:
        require(_name(namespace) in self._namespaces)
        path = "/api/v1/namespaces/" + namespace + "/pods"
        return path if name is None else path + "/" + _name(name)

    async def create_pod(self, namespace: str, document: dict[str, Any]) -> dict[str, Any]:
        created = await self._json("POST", self._pods(namespace), document)
        require(created is not None)
        assert created is not None
        return created

    async def read_pod(self, namespace: str, name: str) -> dict[str, Any] | None:
        return await self._json("GET", self._pods(namespace, name))

    async def pod_log(self, namespace: str, name: str) -> str:
        status, body = await self._send("GET", self._pods(namespace, name) + "/log")
        require(status == 200)
        return body.decode("utf-8", errors="replace")

    async def delete_pod(self, namespace: str, name: str, uid: str) -> None:
        status, _body = await self._send(
            "DELETE",
            self._pods(namespace, name),
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "preconditions": {"uid": uid},
                "gracePeriodSeconds": 0,
            },
        )
        require(200 <= status < 300 or status in (404, 409))

    async def dry_run_create_pod(
        self, namespace: str, document: dict[str, Any]
    ) -> tuple[int | None, str]:
        status, body = await self._send("POST", self._pods(namespace) + "?dryRun=All", document)
        try:
            message = str(json.loads(body).get("message", "")) if status >= 300 else ""
        except (ValueError, AttributeError):
            message = ""
        return status, message

    async def access_review(
        self, user: str, groups: Sequence[str], attributes: ResourceAttributes
    ) -> bool | None:
        review = await self._json(
            "POST",
            "/apis/authorization.k8s.io/v1/subjectaccessreviews",
            {
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SubjectAccessReview",
                "spec": {
                    "user": user,
                    "groups": list(groups),
                    "resourceAttributes": attributes.document(),
                },
            },
        )
        status = None if review is None else review.get("status", {})
        if not isinstance(status, dict) or status.get("evaluationError"):
            return None
        allowed = status.get("allowed")
        return allowed if type(allowed) is bool else None

    def _policies(self, namespace: str, name: str | None = None) -> str:
        require(_name(namespace) in self._policy_namespaces)
        path = "/apis/networking.k8s.io/v1/namespaces/" + namespace + "/networkpolicies"
        return path if name is None else path + "/" + _name(name)

    async def create_network_policy(
        self, namespace: str, document: dict[str, Any]
    ) -> dict[str, Any]:
        created = await self._json("POST", self._policies(namespace), document)
        require(created is not None)
        assert created is not None
        return created

    async def delete_network_policy(self, namespace: str, name: str, uid: str) -> None:
        require(isinstance(uid, str) and bool(uid))
        status, _body = await self._send(
            "DELETE", self._policies(namespace, name),
            {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": uid}},
        )
        require(200 <= status < 300 or status in (404, 409))

    async def list_items(self, collection: str) -> list[dict[str, Any]] | None:
        if collection.startswith("networkpolicies:"):
            namespace = _name(collection.split(":", 1)[1])
            path = "/apis/networking.k8s.io/v1/namespaces/" + namespace + "/networkpolicies"
        elif collection in _LISTS:
            path = _LISTS[collection]
        else:
            require(_PROVIDER.fullmatch(collection) is not None)
            path = collection
        items: list[dict[str, Any]] = []
        token = ""
        while True:
            query = "?limit=500" + ("&continue=" + quote(token, safe="") if token else "")
            try:
                page = await self._json("GET", path + query)
            except ProbeKitRefused:
                return None
            if page is None or type(page.get("items")) is not list:
                return None
            items += page["items"]
            token = page.get("metadata", {}).get("continue", "")
            if not token:
                return items

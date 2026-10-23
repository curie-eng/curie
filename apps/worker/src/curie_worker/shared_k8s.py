"""The shared reconciler's client against a real cluster (ADR 0239, cp5).

Separate from ``connector_k8s`` on purpose: a shared application's objects are
not any agent's, so nothing here lists by ``curie.dev/connector-owner``, and
the verbs are narrower than the dedicated client's.

1. Deployments, Services and NetworkPolicies are server-side applied with the
   field manager ``curie-shared-connector-reconciler`` (``force=True``, the
   same drift correction ``connector_k8s`` documents), listed by label and
   deleted by name. A Deployment is deleted with ``Foreground`` propagation,
   so the API server keeps it listed, with a ``deletionTimestamp``, until its
   ReplicaSets and Pods are gone: the reconciler deletes an application's
   Service and NetworkPolicies only once its Deployment is no longer listed,
   with no Pod verb in the worker's Role.
2. Secrets are never applied, patched or deleted. The worker's own key and
   DSN Secrets are created once (``POST``; ``409 AlreadyExists`` answers
   ``"exists"`` and changes nothing) and read back by an exact ``list``
   (labels plus ``metadata.name``), the one Secret verb besides ``create``
   the worker's Role grants: it has no ``get``.
3. An operator's Secret (the grant store, the snapshot seal, a managed DSN)
   is only checked for presence, by a metadata-only list
   (``PartialObjectMetadataList``), so none of its data reaches this process.

Kubeconfig loading is deferred to the first call: the dedicated client beside
this one loads the same configuration at boot, so a bad kubeconfig still fails
there, next to the flag that asked for the loop.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Literal

from kubernetes import client as k8s_client

from .connector_k8s import _load_cluster_config
from .connector_reconcile import OWNER_LABEL

FIELD_MANAGER = "curie-shared-connector-reconciler"

_KINDS: dict[str, tuple[type[Any], str]] = {
    "Deployment": (k8s_client.AppsV1Api, "deployment"),
    "Service": (k8s_client.CoreV1Api, "service"),
    "NetworkPolicy": (k8s_client.NetworkingV1Api, "network_policy"),
}
SHARED_KINDS = tuple(_KINDS)
_METADATA_ONLY = "application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1"


class SharedObjectRefused(ValueError):
    """A write the shared client never makes: a Secret, or a dedicated-owned object."""


def selector_text(selector: str | Mapping[str, str]) -> str:
    if isinstance(selector, str):
        return selector
    return ",".join(f"{key}={value}" for key, value in sorted(selector.items()))


class KubernetesSharedClient:
    """The shared reconciler's cluster surface (in-cluster or kubeconfig auth)."""

    def __init__(self, *, kubeconfig: str | None = None) -> None:
        self._kubeconfig = kubeconfig
        self._loaded = False
        self._apis: dict[type[Any], Any] = {}

    def _api(self, api_cls: type[Any]) -> Any:
        if not self._loaded:
            _load_cluster_config(self._kubeconfig)
            self._loaded = True
        if api_cls not in self._apis:
            self._apis[api_cls] = api_cls()
        return self._apis[api_cls]

    def _kind(self, kind: str) -> tuple[Any, str]:
        if kind not in _KINDS:
            raise SharedObjectRefused(f"{kind} is not a shared application object kind")
        api_cls, suffix = _KINDS[kind]
        return self._api(api_cls), suffix

    # -- Deployments, Services, NetworkPolicies ---------------------------------

    def list_labelled(
        self, namespace: str, kind: str, selector: str | Mapping[str, str]
    ) -> list[dict[str, Any]]:
        """Objects of ``kind`` the server selects; items carry no ``kind``, as listed."""

        api, suffix = self._kind(kind)
        response = getattr(api, f"list_namespaced_{suffix}")(
            namespace, label_selector=selector_text(selector), _preload_content=False
        )
        items: list[dict[str, Any]] = json.loads(response.data).get("items", [])
        return items

    def apply(self, namespace: str, obj: dict[str, Any]) -> None:
        kind = str(obj.get("kind", ""))
        if OWNER_LABEL in ((obj.get("metadata") or {}).get("labels") or {}):
            raise SharedObjectRefused("a shared object never carries the dedicated owner label")
        api, suffix = self._kind(kind)
        getattr(api, f"patch_namespaced_{suffix}")(
            obj["metadata"]["name"],
            namespace,
            obj,
            field_manager=FIELD_MANAGER,
            force=True,
            _content_type="application/apply-patch+yaml",
            _preload_content=False,
        )

    def delete(self, namespace: str, kind: str, name: str) -> None:
        api, suffix = self._kind(kind)
        # Foreground for a Deployment (point 1); the server's default otherwise.
        options: dict[str, Any] = (
            {"propagation_policy": "Foreground"} if kind == "Deployment" else {}
        )
        try:
            getattr(api, f"delete_namespaced_{suffix}")(
                name, namespace, _preload_content=False, **options
            )
        except k8s_client.ApiException as exc:
            # Already gone is the state we wanted. Anything else is real.
            if exc.status != 404:
                raise

    # -- Secrets ------------------------------------------------------------------

    def create_secret(self, namespace: str, body: dict[str, Any]) -> Literal["created", "exists"]:
        """Create once. An existing Secret of that name is left exactly as it is."""

        if "stringData" in body:
            raise SharedObjectRefused("a worker Secret is created from data only")
        api = self._api(k8s_client.CoreV1Api)
        try:
            api.create_namespaced_secret(namespace, body, _preload_content=False)
        except k8s_client.ApiException as exc:
            if exc.status == 409:
                return "exists"
            raise
        return "created"

    def list_secret(
        self, namespace: str, name: str, labels: Mapping[str, str]
    ) -> dict[str, Any] | None:
        """The one Secret named ``name`` carrying exactly ``labels``, or None."""

        api = self._api(k8s_client.CoreV1Api)
        response = api.list_namespaced_secret(
            namespace,
            label_selector=selector_text(labels),
            field_selector=f"metadata.name={name}",
            _preload_content=False,
        )
        items = json.loads(response.data).get("items", [])
        named = [item for item in items if (item.get("metadata") or {}).get("name") == name]
        return named[0] if len(named) == 1 else None

    def secret_exists_metadata_only(self, namespace: str, name: str) -> bool:
        """Whether ``name`` exists, read as object metadata only: no data is transferred."""

        api_client = self._api(k8s_client.CoreV1Api).api_client
        # The generated list method fixes its own Accept header, so the
        # metadata-only representation is asked for on the raw call.
        response = api_client.call_api(
            "/api/v1/namespaces/{namespace}/secrets",
            "GET",
            path_params={"namespace": namespace},
            query_params=[("fieldSelector", f"metadata.name={name}")],
            header_params={"Accept": _METADATA_ONLY},
            auth_settings=["BearerToken"],
            _return_http_data_only=True,
            _preload_content=False,
        )
        listed = json.loads(response.data)
        if listed.get("kind") != "PartialObjectMetadataList":
            raise SharedObjectRefused("the API server did not answer metadata only")
        return any(
            (item.get("metadata") or {}).get("name") == name for item in listed.get("items", [])
        )

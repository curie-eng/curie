"""The `ConnectorClient` against a real cluster (ADR-0090).

Four object kinds, three verbs, one namespace. Everything Kubernetes-shaped in
the connector reconciler stops here; `connector_reconcile` and
`connector_apply` never import this module, which is what lets the dangerous
half be tested without a cluster.

**Writes are server-side applies.** Not create-then-replace-on-409, which is
where the obvious implementation goes wrong: a rendered Service declares no
`clusterIP`, and replacing a live Service without one is rejected outright
(`spec.clusterIP: Invalid value: "": field is immutable`). Server-side apply
also removes a field we previously set and no longer declare, which a merge
patch cannot express -- so dropping `hostAliases` from connectors.yaml actually
drops it from the pod, rather than leaving it in place forever.

**`force=True` is deliberate.** It means a field a human took ownership of with
`kubectl edit` comes back to us. That IS the drift correction ADR-0090 asks
for; without it, an edited field stays edited and the reconciler reports
converged while the cluster disagrees with the declaration.

**Reads go out as raw JSON, not typed models.** The rest of the reconciler
compares against what the API rendered, which is camelCase; the generated
models would hand back snake_case attributes and quietly compare unequal on
every field. Asking for the wire format is what keeps the two halves speaking
the same language.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

from .connector_reconcile import OWNER_LABEL

logger = logging.getLogger(__name__)

# Identifies this writer to the API server's field-ownership tracking. A stable
# name matters: change it and the server treats every field as newly claimed by
# a stranger, leaving the old manager's entries behind forever.
FIELD_MANAGER = "curie-connector-reconciler"
LAST_APPLIED_ANNOTATION = "kubectl.kubernetes.io/last-applied-configuration"

# The only kinds a connector is made of, and the only kinds the Role grants.
# Adding one here without adding it to the chart's RBAC produces a reconciler
# that silently fails to prune -- so they are listed in one place each and
# asserted against each other in the chart tests.
_KINDS: dict[str, tuple[type[Any], str, str]] = {
    "Deployment": (k8s_client.AppsV1Api, "deployment", "apps/v1"),
    "Service": (k8s_client.CoreV1Api, "service", "v1"),
    "Secret": (k8s_client.CoreV1Api, "secret", "v1"),
    "NetworkPolicy": (k8s_client.NetworkingV1Api, "network_policy", "networking.k8s.io/v1"),
}

CONNECTOR_KINDS = tuple(_KINDS)


class UnsupportedKind(ValueError):
    """A plan named an object kind connectors are not made of."""


def _load_cluster_config(kubeconfig: str | None) -> None:
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config(config_file=kubeconfig)


def connector_deployments_api(*, kubeconfig: str | None = None) -> k8s_client.AppsV1Api:
    """The apps API the digest-attributing recorder reads one Deployment through.

    Same credential as the reconciler. @spec ACTION-EXECUTOR-12: the recorder
    uses only ``read_namespaced_deployment`` on it, the ``get`` the chart grants
    with the executor enabled -- never this module's ``list_owned``.
    """

    _load_cluster_config(kubeconfig)
    # One attempt per read. The recorder abandons a read at its two second
    # bound, but the sync client keeps running in its thread; urllib3's default
    # Retry(3) would hold that thread for several more bounds against a stalled
    # API server. With no retries, the per-request total timeout the recorder
    # passes ends the thread at the bound too.
    configuration = k8s_client.Configuration.get_default_copy()
    configuration.retries = 0
    return k8s_client.AppsV1Api(k8s_client.ApiClient(configuration))


class KubernetesConnectorClient:
    """ConnectorClient against a real cluster (in-cluster or kubeconfig auth)."""

    def __init__(self, *, kubeconfig: str | None = None) -> None:
        _load_cluster_config(kubeconfig)
        self._apis: dict[str, Any] = {}

    def _api(self, kind: str) -> tuple[Any, str]:
        if kind not in _KINDS:
            raise UnsupportedKind(f"{kind} is not a connector object kind")
        api_cls, suffix, _ = _KINDS[kind]
        if kind not in self._apis:
            self._apis[kind] = api_cls()
        return self._apis[kind], suffix

    # -- reads ---------------------------------------------------------------

    def list_owned(self, namespace: str, owner: str) -> list[dict[str, Any]]:
        """Every connector object in the namespace labelled for this agent.

        The label selector is the ownership boundary, and it is applied by the
        API server rather than by us: a client-side filter over a wider list is
        a filter that can be forgotten, and forgetting it here prunes another
        agent's connectors (#1116).
        """

        found: list[dict[str, Any]] = []
        for kind in _KINDS:
            api, suffix = self._api(kind)
            response = getattr(api, f"list_namespaced_{suffix}")(
                namespace,
                label_selector=f"{OWNER_LABEL}={owner}",
                _preload_content=False,
            )
            for item in json.loads(response.data).get("items", []):
                # Items inside a List carry no `kind` or `apiVersion` of their
                # own -- the server states them once on the envelope. The plan
                # identifies objects by (kind, name), so an unstamped item is an
                # object with an empty kind that matches nothing and gets
                # planned for deletion.
                item["kind"] = kind
                item["apiVersion"] = _KINDS[kind][2]
                if kind == "Secret" and LAST_APPLIED_ANNOTATION in (
                    item.get("metadata", {}).get("annotations") or {}
                ):
                    self._strip_last_applied(
                        namespace,
                        item["metadata"]["name"],
                        uid=item["metadata"]["uid"],
                        owner=owner,
                    )
                    item["metadata"]["annotations"].pop(LAST_APPLIED_ANNOTATION, None)
                    logger.warning(
                        "connector credential Secret carried kubectl last-applied metadata; "
                        "removed stale plaintext copy owner=%s namespace=%s; rotate the credential "
                        "if the annotation may have exposed it",
                        owner,
                        namespace,
                    )
                found.append(item)
        return found

    # -- writes --------------------------------------------------------------

    def _strip_last_applied(self, namespace: str, name: str, *, uid: str, owner: str) -> None:
        api, suffix = self._api("Secret")
        getattr(api, f"patch_namespaced_{suffix}")(
            name,
            namespace,
            [
                {"op": "test", "path": "/metadata/uid", "value": uid},
                {
                    "op": "test",
                    "path": "/metadata/labels/curie.dev~1connector-owner",
                    "value": owner,
                },
                {
                    "op": "remove",
                    "path": (
                        "/metadata/annotations/"
                        "kubectl.kubernetes.io~1last-applied-configuration"
                    ),
                }
            ],
            _content_type="application/json-patch+json",
            _preload_content=False,
        )

    def apply(self, namespace: str, obj: dict[str, Any]) -> None:
        kind = str(obj.get("kind", ""))
        api, suffix = self._api(kind)
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
        api, suffix = self._api(kind)
        try:
            getattr(api, f"delete_namespaced_{suffix}")(name, namespace, _preload_content=False)
        except k8s_client.ApiException as exc:
            # Already gone is the state we wanted. Anything else is real.
            if exc.status != 404:
                raise

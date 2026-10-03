"""Inherited kubectl annotations must not retain connector credentials (#1419)."""

from __future__ import annotations

import base64
import copy
import json
import logging
from typing import Any

import pytest
from curie_worker.connector_agent import RenderedConnectors, reconcile_agent
from curie_worker.connector_k8s import KubernetesConnectorClient
from curie_worker.connector_reconcile import OWNER_LABEL

AGENT = "acme-bot"
NAMESPACE = "curie"
SECRET = "curie-acme-bot-connector-secrets"
LAST_APPLIED = "kubectl.kubernetes.io/last-applied-configuration"
FOREIGN_ANNOTATION = "example.com/keep"
STALE_VALUE = '{"kind":"Secret","stringData":{"TOKEN":"old-fixture-token"}}'


class Source:
    def __init__(self, rendered: RenderedConnectors) -> None:
        self._rendered = rendered

    def rendered(self, *, agent_id: str, version_id: str) -> RenderedConnectors:
        return self._rendered


class SecretApi:
    """Model only the external API server's Secret read and metadata patch effects."""

    def __init__(self, secrets: list[dict[str, Any]]) -> None:
        self.secrets = {secret["metadata"]["name"]: copy.deepcopy(secret) for secret in secrets}

    def list_namespaced_secret(self, namespace: str, *, label_selector: str, **_: Any) -> Any:
        owner = label_selector.split("=", 1)[1]
        items = [
            copy.deepcopy(secret)
            for secret in self.secrets.values()
            if secret["metadata"].get("labels", {}).get(OWNER_LABEL) == owner
        ]
        return type("Response", (), {"data": json.dumps({"items": items}).encode()})()

    def list_namespaced_deployment(self, namespace: str, **_: Any) -> Any:
        return type("Response", (), {"data": b'{"items":[]}'})()

    list_namespaced_service = list_namespaced_deployment
    list_namespaced_network_policy = list_namespaced_deployment

    def patch_namespaced_secret(
        self, name: str, namespace: str, body: dict[str, Any] | list[dict[str, Any]], **_: Any
    ) -> None:
        stored = self.secrets[name]
        if isinstance(body, list):
            for operation in body:
                if operation["op"] == "test":
                    actual = {
                        "/metadata/uid": stored["metadata"]["uid"],
                        "/metadata/labels/curie.dev~1connector-owner": stored["metadata"][
                            "labels"
                        ][OWNER_LABEL],
                    }[operation["path"]]
                    if actual != operation["value"]:
                        raise RuntimeError("JSON Patch ownership precondition failed")
                if operation["op"] == "remove" and operation["path"] == (
                    "/metadata/annotations/kubectl.kubernetes.io~1last-applied-configuration"
                ):
                    stored["metadata"]["annotations"].pop(LAST_APPLIED, None)
            return
        # Server-side apply preserves fields it does not manage, including an
        # inherited last-applied annotation. A null metadata patch removes it.
        for key, value in body.get("metadata", {}).get("annotations", {}).items():
            if value is None:
                stored["metadata"]["annotations"].pop(key, None)
            else:
                stored["metadata"]["annotations"][key] = value
        for key, value in body.get("stringData", {}).items():
            stored.setdefault("data", {})[key] = base64.b64encode(value.encode()).decode()


def secret(name: str, owner: str, value: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {
            "name": name,
            "uid": f"uid-{name}",
            "labels": {OWNER_LABEL: owner},
            "annotations": {LAST_APPLIED: STALE_VALUE, FOREIGN_ANNOTATION: "kept"},
        },
        "data": {"TOKEN": base64.b64encode(value.encode()).decode()},
    }


def reconcile(api: SecretApi, rendered: RenderedConnectors) -> None:
    # Exercise the real reconciler and Kubernetes client; only the remote API
    # server is replaced by the stateful stand-in above.
    client = KubernetesConnectorClient.__new__(KubernetesConnectorClient)
    client._api = lambda kind: (api, {"NetworkPolicy": "network_policy"}.get(kind, kind.lower()))  # type: ignore[method-assign]
    outcome = reconcile_agent(
        Source(rendered), client, agent=AGENT, agent_id="a-1", version_id="v-1", namespace=NAMESPACE
    )
    assert outcome.ok


def test_protected_secret_scrubs_last_applied_but_preserves_other_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    owned = secret(SECRET, AGENT, "rotated-fixture-token")
    foreign = secret("foreign-secret", "different-agent", "foreign-fixture-token")
    api = SecretApi([owned, foreign])

    with caplog.at_level(logging.WARNING):
        reconcile(api, RenderedConnectors(owned_secret_name=SECRET, owned_secret_keys=["TOKEN"]))

    stored = api.secrets[SECRET]
    annotations = stored["metadata"]["annotations"]
    assert LAST_APPLIED not in annotations
    assert annotations[FOREIGN_ANNOTATION] == "kept"
    assert base64.b64decode(stored["data"]["TOKEN"]) == b"rotated-fixture-token"
    assert api.secrets["foreign-secret"] == foreign
    assert "rotate the credential" in caplog.text
    assert SECRET not in caplog.text
    assert STALE_VALUE not in caplog.text


def test_rotation_updates_owned_secret_and_removes_stale_plaintext_annotation() -> None:
    api = SecretApi([secret(SECRET, AGENT, "old-fixture-token")])
    desired = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {"name": SECRET},
        "stringData": {"TOKEN": "rotated-fixture-token"},
    }

    reconcile(api, RenderedConnectors(manifests=[desired]))

    stored = api.secrets[SECRET]
    assert base64.b64decode(stored["data"]["TOKEN"]) == b"rotated-fixture-token"
    annotations = stored["metadata"]["annotations"]
    assert LAST_APPLIED not in annotations
    assert annotations[FOREIGN_ANNOTATION] == "kept"


def test_scrub_refuses_a_secret_reowned_after_the_filtered_list() -> None:
    # Kubernetes applies JSON Patch `test` operations conditionally with the
    # mutation in one request: https://kubernetes.io/docs/reference/using-api/api-concepts/#patch-operations
    class ReownedApi(SecretApi):
        def patch_namespaced_secret(
            self,
            name: str,
            namespace: str,
            body: dict[str, Any] | list[dict[str, Any]],
            **kwargs: Any,
        ) -> None:
            if isinstance(body, list):
                self.secrets[name]["metadata"]["labels"][OWNER_LABEL] = "different-agent"
            super().patch_namespaced_secret(name, namespace, body, **kwargs)

    api = ReownedApi([secret(SECRET, AGENT, "rotated-fixture-token")])

    with pytest.raises(RuntimeError, match="ownership precondition"):
        reconcile(api, RenderedConnectors(owned_secret_name=SECRET, owned_secret_keys=["TOKEN"]))

    assert LAST_APPLIED in api.secrets[SECRET]["metadata"]["annotations"]

"""Launch object shapes and the fixed launch control refusal.

@spec PROTECTED-HOOK-LANE-2 PROTECTED-HOOK-LANE-8. The launch Secret and Service
the mediator creates, and the finalizer it places, as pure shape builders. They
live apart from the Kubernetes transport so that a reader of these shapes (the
commissioning guard) imports no launch registry or transport.
"""

from __future__ import annotations

import base64
from typing import Any, NoReturn

FINALIZER = "curietech.ai/protected-launch"


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

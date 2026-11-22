"""Recorded kubelet pod shapes and the production Kubernetes client over them (#5427).

``recorded_pod_shapes.json`` holds pods a kubelet actually ended, trimmed to
the fields diagnosis reads. These helpers turn a shape into the client's own
models and build a ``KubernetesSandboxClient`` whose only doubles are the
CoreV1Api and CustomObjectsApi objects handed to it.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from curie_worker.sandbox.k8s import KubernetesSandboxClient
from kubernetes import client as k8s_client

SHAPES: dict[str, Any] = json.loads(
    (Path(__file__).parent / "recorded_pod_shapes.json").read_text()
)["pods"]
MI = 1024 * 1024
NAMESPACE = "test-ns"
POOL = "test-pool"
TEMPLATE = "test-template"


def shape(name: str) -> dict[str, Any]:
    """A private copy of one recorded pod."""

    return copy.deepcopy(SHAPES[name])


def pod(recorded: dict[str, Any]) -> k8s_client.V1Pod:
    """A recorded pod as the client's own V1Pod model."""

    body = {key: value for key, value in recorded.items() if key != "events"}
    # The recording trimmed fields diagnosis never reads; the client model
    # requires these four on every container status, so they come back as the
    # API server always sends them.
    for status in body["status"].get("containerStatuses", []):
        status.setdefault("image", "runner:recorded")
        status.setdefault("imageID", "runner@sha256:" + "0" * 64)
        status.setdefault("ready", False)
        status.setdefault("restartCount", 0)
    model: k8s_client.V1Pod = k8s_client.ApiClient()._ApiClient__deserialize(body, "V1Pod")  # type: ignore[attr-defined]
    return model


def event(raw: dict[str, Any], *, uid: str | None, at: datetime) -> Any:
    """A recorded event as the client's own model, about the pod with ``uid``."""

    body = {
        **raw,
        "metadata": {"name": "event"},
        "involvedObject": {**raw["involvedObject"], "uid": uid},
        "lastTimestamp": at.isoformat(),
    }
    return k8s_client.ApiClient()._ApiClient__deserialize(body, "CoreV1Event")  # type: ignore[attr-defined]


# The core/v1 Event field selectors the API server supports that the client
# sends, each as the field it reads on the client's own model.
_EVENT_SELECTOR_FIELDS: dict[str, Callable[[Any], object]] = {
    "involvedObject.kind": lambda item: item.involved_object.kind,
    "involvedObject.name": lambda item: item.involved_object.name,
    "involvedObject.uid": lambda item: item.involved_object.uid,
    "reason": lambda item: item.reason,
}


def _first_seen(item: Any) -> datetime:
    when = (
        item.first_timestamp
        or item.last_timestamp
        or item.event_time
        or item.metadata.creation_timestamp
    )
    assert isinstance(when, datetime), item
    return when


def event_page(events: Iterable[Any], *, field_selector: str, limit: int) -> Any:
    """The one page the API server answers a field-selected event list with.

    Events are named ``<involvedObject.name>.<hex unix nanos>`` and listed in
    key order, so a name's events come oldest first. The client follows no
    continue token, so only the first ``limit`` matches reach it. A selector
    key outside the supported set fails the test instead of matching all.
    """

    wanted: dict[str, str] = {}
    for term in field_selector.split(","):
        key, equals, value = term.partition("=")
        assert equals and key in _EVENT_SELECTOR_FIELDS, f"unsupported selector {term!r}"
        wanted[key] = value
    matches = [
        item
        for item in events
        if all(_EVENT_SELECTOR_FIELDS[key](item) == value for key, value in wanted.items())
    ]
    matches.sort(key=_first_seen)
    return k8s_client.CoreV1EventList(items=matches[:limit])


def startup_events(pod_name: str, containers: list[str], *, uid: str, start: datetime) -> list[Any]:
    """A pod's startup events, one second apart from ``start``, as the kubelet writes them."""

    raws = [("Scheduled", "Successfully assigned the pod to a node")]
    for container in containers:
        raws += [
            ("Pulling", f'Pulling image "{container}:recorded"'),
            ("Pulled", f'Successfully pulled image "{container}:recorded"'),
            ("Created", f"Created container: {container}"),
            ("Started", f"Started container {container}"),
        ]
    return [
        event(
            {
                "reason": reason,
                "message": message,
                "type": "Normal",
                "involvedObject": {"kind": "Pod", "name": pod_name},
            },
            uid=uid,
            at=start + timedelta(seconds=index),
        )
        for index, (reason, message) in enumerate(raws)
    ]


def client(api: Any) -> KubernetesSandboxClient:
    """The production client over ``api``, which answers both API groups."""

    built = KubernetesSandboxClient.__new__(KubernetesSandboxClient)
    built._api = api  # type: ignore[attr-defined]
    built._core_api = api  # type: ignore[attr-defined]
    built._namespace = NAMESPACE  # type: ignore[attr-defined]
    return built


def templates(spec: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    """A pool and the template it names, as the API server stores them."""

    return {
        ("sandboxwarmpools", POOL): {"spec": {"sandboxTemplateRef": {"name": TEMPLATE}}},
        ("sandboxtemplates", TEMPLATE): {"spec": {"podTemplate": {"spec": spec}}},
    }

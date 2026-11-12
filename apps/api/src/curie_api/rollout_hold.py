"""Hold personal enrollment and admission during an API rollout (#5377).

Migration 0160 is an expand migration, so during a rolling upgrade an older API
replica can serve the same database. Its ``settle_terminal`` would expire the
transcript of a personal request this release keeps after an enrollment fence
or an authentication failure. Until every API replica of this installation
runs one pod template, owner enrollment writes (enroll, replace, reconnect)
and personal trusted admission answer ``personal_rollout_in_progress`` and
write nothing. Disconnect is never held: it only reduces access.

The check lists the API pods of this pod's namespace through the read-only
pod listing the API already holds (``pods: list``), finds this pod by its
name (``HOSTNAME``), and requires every Running or Pending API pod of the same
release to carry this pod's ``pod-template-hash``. A mismatch, a failed list,
or no in-cluster configuration means held. ``CURIE_PERSONAL_ROLLOUT_HOLD``
false turns the hold off; single-instance stacks and the test suite set it so
explicitly.

``ApiPodSource`` is the seam tests fake; the real one reads through a private
``ApiClient`` with no retries and a request timeout. Nothing here logs a
Kubernetes response body.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from .config import Settings

logger = logging.getLogger(__name__)

HOLD_CODE = "personal_rollout_in_progress"
COMPONENT_LABEL = "app.kubernetes.io/component"
INSTANCE_LABEL = "app.kubernetes.io/instance"
NAME_LABEL = "app.kubernetes.io/name"
TEMPLATE_HASH_LABEL = "pod-template-hash"
API_POD_SELECTOR = f"{COMPONENT_LABEL}=api"
_SERVING_PHASES = frozenset({"Running", "Pending"})
_SERVICE_ACCOUNT_NAMESPACE = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
_LIST_TIMEOUT_S = 5.0


@dataclass(frozen=True)
class ApiPod:
    name: str
    phase: str | None
    labels: Mapping[str, str] = field(default_factory=dict)


class ApiPodSource(Protocol):
    def list_api_pods(self, namespace: str, label_selector: str) -> list[ApiPod]:
        """The pods matching ``label_selector``; any failure raises."""
        ...


class KubernetesApiPodSource:
    """``ApiPodSource`` over one CoreV1 client built on first use."""

    def __init__(self, core_factory: Any, *, timeout_s: float = _LIST_TIMEOUT_S) -> None:
        self._core_factory = core_factory
        self._core: Any = None
        self._timeout_s = timeout_s

    def list_api_pods(self, namespace: str, label_selector: str) -> list[ApiPod]:
        if self._core is None:
            self._core = self._core_factory()
        listed = self._core.list_namespaced_pod(
            namespace, label_selector=label_selector, _request_timeout=self._timeout_s
        )
        return [
            ApiPod(
                name=str(item.metadata.name),
                phase=getattr(item.status, "phase", None),
                labels=dict(item.metadata.labels or {}),
            )
            for item in listed.items
        ]


def single_template(pods: Iterable[ApiPod], own_name: str) -> bool:
    """Whether every serving API pod of this pod's release shares its template."""

    listed = list(pods)
    own = next((pod for pod in listed if pod.name == own_name), None)
    if own is None:
        return False
    template = own.labels.get(TEMPLATE_HASH_LABEL)
    release = (own.labels.get(NAME_LABEL), own.labels.get(INSTANCE_LABEL))
    if not template or not release[1]:
        return False
    for pod in listed:
        if (pod.labels.get(NAME_LABEL), pod.labels.get(INSTANCE_LABEL)) != release:
            continue
        if pod.phase not in _SERVING_PHASES:
            continue
        if pod.labels.get(TEMPLATE_HASH_LABEL) != template:
            return False
    return True


@dataclass(frozen=True)
class RolloutHold:
    """Answers whether personal writes are held right now. Fails closed."""

    enabled: bool
    pods: ApiPodSource | None = None
    namespace: str | None = None
    pod_name: str | None = None

    async def held(self) -> bool:
        if not self.enabled:
            return False
        if self.pods is None or not self.namespace or not self.pod_name:
            return True
        try:
            pods = await asyncio.to_thread(
                self.pods.list_api_pods, self.namespace, API_POD_SELECTOR
            )
        except Exception as exc:  # noqa: BLE001 - any failure holds; text unread
            logger.warning("personal rollout hold: API pod list failed (%s)", type(exc).__name__)
            return True
        return not single_template(pods, self.pod_name)


def _in_cluster_core() -> Any:
    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config

    configuration = k8s_client.Configuration()
    k8s_config.load_incluster_config(client_configuration=configuration)  # type: ignore[no-untyped-call]
    configuration.retries = 0
    return k8s_client.CoreV1Api(k8s_client.ApiClient(configuration=configuration))


def build_rollout_hold(settings: Settings) -> RolloutHold:
    """The installation's hold: in-cluster identity and pod listing, or held."""

    if not settings.personal_rollout_hold:
        return RolloutHold(enabled=False)
    try:
        namespace = _SERVICE_ACCOUNT_NAMESPACE.read_text(encoding="utf-8").strip()
    except OSError:
        namespace = ""
    pod_name = os.environ.get("HOSTNAME", "").strip()
    if not namespace or not pod_name:
        return RolloutHold(enabled=True)
    return RolloutHold(
        enabled=True,
        pods=KubernetesApiPodSource(_in_cluster_core),
        namespace=namespace,
        pod_name=pod_name,
    )


_installed: RolloutHold | None = None


def install(hold: RolloutHold) -> None:
    """Make ``hold`` the one personal admission consults (the app lifespan calls this)."""

    global _installed
    _installed = hold


def uninstall(hold: RolloutHold) -> None:
    global _installed
    if _installed is hold:
        _installed = None


async def personal_rollout_held() -> bool:
    """Whether personal writes are held now. Without an installed hold: held."""

    from .config import get_settings

    if not get_settings().personal_rollout_hold:
        return False
    hold = _installed
    if hold is None:
        return True
    return await hold.held()

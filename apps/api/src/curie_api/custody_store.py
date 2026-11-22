"""Kubernetes custody of personal Claude grants (#5377, ADR 0268 section 4).

Each enrollment generation's grant lives in one immutable ``Opaque`` Secret in
the operator's dedicated grant namespace, named
``curie-pe-<enrollment uuid as 32 lowercase hex>-g<generation>``, whose only
data key is ``CLAUDE_CODE_OAUTH_TOKEN``. Labels mark it as custody material and
name its enrollment and generation; the ``curietech.ai/personal-custody-id``
annotation names the ``personal_claude_custody`` row it belongs to. The worker's
reader (``curie_worker.personal_custody``) checks the same shape.

``CustodySecretStore`` is the seam the protocol in ``personal_enrollments`` and
its tests use, in the style of the pod-log reader in ``k8s.py``. Its methods are
synchronous; callers run them with ``asyncio.to_thread`` and never inside a
database transaction. Every delete carries a UID precondition. A Kubernetes
``ApiException`` becomes a ``CustodyStoreError`` carrying only the status code:
its text and response body are never logged, raised or chained.

The real store talks through a private ``ApiClient`` built from
``Settings.kube_config_path`` or in-cluster config, with ``retries = 0`` and a
request timeout, never the process-global client configuration.
"""

from __future__ import annotations

import base64
import threading
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, Protocol

if TYPE_CHECKING:
    from .config import Settings

#: The sole data key, the literal migration 0160's ``personal_claude_custody_key_ck`` pins.
CUSTODY_SECRET_KEY = "CLAUDE_CODE_OAUTH_TOKEN"
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
CUSTODY_LABEL = "curietech.ai/personal-custody"
ENROLLMENT_LABEL = "curietech.ai/personal-enrollment"
GENERATION_LABEL = "curietech.ai/personal-generation"
CUSTODY_ID_ANNOTATION = "curietech.ai/personal-custody-id"
#: Only Secrets carrying this label are ever listed or swept.
CUSTODY_SELECTOR = f"{CUSTODY_LABEL}=true"

DeleteOutcome = Literal["deleted", "absent", "uid_mismatch"]


def custody_secret_name(enrollment_id: uuid.UUID, generation: int) -> str:
    """The shared name; the same expression ``personal_claude_custody_name_ck`` pins."""

    return f"curie-pe-{enrollment_id.hex}-g{generation}"


def custody_labels(enrollment_id: uuid.UUID, generation: int) -> dict[str, str]:
    return {
        MANAGED_BY_LABEL: "curie",
        CUSTODY_LABEL: "true",
        ENROLLMENT_LABEL: enrollment_id.hex,
        GENERATION_LABEL: str(generation),
    }


def custody_annotations(custody_id: uuid.UUID) -> dict[str, str]:
    return {CUSTODY_ID_ANNOTATION: str(custody_id)}


@dataclass(frozen=True)
class StoredSecret:
    """What a read or list returned about one Secret. Never its data values."""

    name: str
    uid: str
    labels: Mapping[str, str] = field(default_factory=dict)
    annotations: Mapping[str, str] = field(default_factory=dict)
    created_at: datetime | None = None
    immutable: bool | None = None
    type: str | None = None
    keys: frozenset[str] = frozenset()

    @property
    def custody_id(self) -> uuid.UUID | None:
        """The custody row the annotation names, or None when absent or malformed."""

        raw = self.annotations.get(CUSTODY_ID_ANNOTATION)
        if not raw:
            return None
        try:
            return uuid.UUID(raw)
        except ValueError:
            return None


class CustodyStoreError(Exception):
    """A Secret request failed. Carries the HTTP status only, never a response body."""

    def __init__(self, status: int | None) -> None:
        self.status = status
        super().__init__(
            "custody secret request failed"
            if status is None
            else f"custody secret request failed with HTTP {status}"
        )


class SecretExists(CustodyStoreError):
    """A create named a Secret that already exists (HTTP 409 AlreadyExists)."""

    def __init__(self) -> None:
        super().__init__(409)


class CustodySecretStore(Protocol):
    def create(
        self,
        namespace: str,
        name: str,
        *,
        token: str,
        labels: Mapping[str, str],
        annotations: Mapping[str, str],
    ) -> str:
        """Create the immutable Opaque Secret and return its UID.

        ``SecretExists`` when the name is taken, ``CustodyStoreError`` otherwise.
        """
        ...

    def read(self, namespace: str, name: str) -> StoredSecret | None:
        """The Secret, or None when it is absent (404)."""
        ...

    def delete(self, namespace: str, name: str, *, uid: str) -> DeleteOutcome:
        """Delete only when the Secret still has ``uid``.

        ``absent`` on 404, ``uid_mismatch`` when the name holds another Secret
        (409, the UID precondition failed).
        """
        ...

    def list(self, namespace: str) -> list[StoredSecret]:
        """Every Secret in ``namespace`` carrying the custody label."""
        ...


def _stored(secret: Any) -> StoredSecret:
    metadata = secret.metadata
    return StoredSecret(
        name=str(metadata.name),
        uid=str(metadata.uid),
        labels=dict(metadata.labels or {}),
        annotations=dict(metadata.annotations or {}),
        created_at=metadata.creation_timestamp,
        immutable=secret.immutable,
        type=secret.type,
        keys=frozenset(secret.data or {}),
    )


class KubernetesCustodySecretStore:
    """``CustodySecretStore`` over a CoreV1 client the factory builds on first use."""

    def __init__(self, core_factory: Callable[[], Any], *, timeout_s: float) -> None:
        self._core_factory = core_factory
        self._timeout_s = timeout_s
        self._core: Any = None
        self._lock = threading.Lock()

    def _client(self) -> Any:
        with self._lock:
            if self._core is None:
                try:
                    self._core = self._core_factory()
                except Exception:  # noqa: BLE001 - configuration text is never surfaced
                    raise CustodyStoreError(None) from None
            return self._core

    def create(
        self,
        namespace: str,
        name: str,
        *,
        token: str,
        labels: Mapping[str, str],
        annotations: Mapping[str, str],
    ) -> str:
        from kubernetes import client as k8s_client
        from kubernetes.client.exceptions import ApiException

        body = k8s_client.V1Secret(
            metadata=k8s_client.V1ObjectMeta(
                name=name, labels=dict(labels), annotations=dict(annotations)
            ),
            type="Opaque",
            immutable=True,
            data={CUSTODY_SECRET_KEY: base64.b64encode(token.encode("ascii")).decode("ascii")},
        )
        core = self._client()
        try:
            created = core.create_namespaced_secret(
                namespace, body, _request_timeout=self._timeout_s
            )
        except ApiException as exc:
            if exc.status == 409:
                raise SecretExists() from None
            raise CustodyStoreError(exc.status) from None
        except Exception:  # noqa: BLE001 - transport failures, text unread
            raise CustodyStoreError(None) from None
        uid = getattr(created.metadata, "uid", None)
        if not uid:
            raise CustodyStoreError(None)
        return str(uid)

    def read(self, namespace: str, name: str) -> StoredSecret | None:
        from kubernetes.client.exceptions import ApiException

        core = self._client()
        try:
            secret = core.read_namespaced_secret(name, namespace, _request_timeout=self._timeout_s)
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise CustodyStoreError(exc.status) from None
        except Exception:  # noqa: BLE001 - transport failures, text unread
            raise CustodyStoreError(None) from None
        return _stored(secret)

    def delete(self, namespace: str, name: str, *, uid: str) -> DeleteOutcome:
        from kubernetes import client as k8s_client
        from kubernetes.client.exceptions import ApiException

        core = self._client()
        try:
            core.delete_namespaced_secret(
                name,
                namespace,
                body=k8s_client.V1DeleteOptions(preconditions=k8s_client.V1Preconditions(uid=uid)),
                _request_timeout=self._timeout_s,
            )
        except ApiException as exc:
            if exc.status == 404:
                return "absent"
            if exc.status == 409:
                return "uid_mismatch"
            raise CustodyStoreError(exc.status) from None
        except Exception:  # noqa: BLE001 - transport failures, text unread
            raise CustodyStoreError(None) from None
        return "deleted"

    def list(self, namespace: str) -> list[StoredSecret]:
        from kubernetes.client.exceptions import ApiException

        core = self._client()
        try:
            listed = core.list_namespaced_secret(
                namespace, label_selector=CUSTODY_SELECTOR, _request_timeout=self._timeout_s
            )
        except ApiException as exc:
            raise CustodyStoreError(exc.status) from None
        except Exception:  # noqa: BLE001 - transport failures, text unread
            raise CustodyStoreError(None) from None
        return [_stored(item) for item in listed.items]


def private_core_v1(kube_config_path: str | None) -> Any:
    """A CoreV1 client on its own ``Configuration``: kubeconfig when named, else in-cluster.

    The global default configuration other clients copy is never touched, and
    ``retries = 0`` makes the request timeout the whole wait.
    """

    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config

    configuration = k8s_client.Configuration()
    if kube_config_path:
        k8s_config.load_kube_config(
            config_file=kube_config_path, client_configuration=configuration
        )  # type: ignore[no-untyped-call]
    else:
        k8s_config.load_incluster_config(client_configuration=configuration)  # type: ignore[no-untyped-call]
    configuration.retries = 0
    return k8s_client.CoreV1Api(k8s_client.ApiClient(configuration=configuration))


def build_custody_secret_store(settings: Settings) -> CustodySecretStore | None:
    """The real store, or None when no grant namespace is configured.

    The cluster configuration loads on the first request, so an absent
    credential answers ``custody_unavailable`` there instead of failing boot.
    """

    if not settings.personal_grant_namespace:
        return None
    path = settings.kube_config_path
    return KubernetesCustodySecretStore(
        lambda: private_core_v1(path),
        timeout_s=settings.personal_custody_kube_timeout_seconds,
    )
